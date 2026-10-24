from django.contrib import admin
from django.contrib.auth import get_user_model
from django.test import RequestFactory, TestCase
from rest_framework.test import APIClient

from .models import Follow, Friendship, Profile, UserVisibilityBlock


User = get_user_model()


class UserDiscoveryTests(TestCase):
    def setUp(self):
        self.viewer = User.objects.create_user("viewer", password="Valid-password-49!")
        self.normal = User.objects.create_user("FindNormal")
        self.staff = User.objects.create_user("FindStaff", password="Valid-password-49!", is_staff=True)
        self.superuser = User.objects.create_user("FindSuperuser", password="Valid-password-49!", is_superuser=True)
        self.both = User.objects.create_superuser("FindBoth", "admin@example.com", "Valid-password-49!")
        self.internal = [self.staff, self.superuser, self.both]
        self.client = APIClient()
        self.client.force_authenticate(self.viewer)

    def usernames(self, response):
        self.assertEqual(response.status_code, 200)
        data = response.json()
        rows = data["results"] if isinstance(data, dict) else data
        return [row["username"] for row in rows]

    def befriend(self, user):
        return Friendship.objects.create(requester=self.viewer, user1=self.viewer, user2=user, status=Friendship.STATUS_ACCEPTED)

    def test_search_excludes_each_internal_flag_and_preserves_normal_match(self):
        for path in ["/api/users/search/", "/api/users/search"]:
            self.assertEqual(self.usernames(self.client.get(path, {"q": " @fInD "})), [self.normal.username])
            for user in self.internal:
                self.assertEqual(self.usernames(self.client.get(path, {"q": user.username})), [])

    def test_search_preserves_bilateral_restrictions_and_private_profile_discoverability(self):
        self.normal.profile.visibility = Profile.Visibility.PRIVATE
        self.normal.profile.is_public = False
        self.normal.profile.save()
        # Existing search allows discovering private profiles; reading stays protected.
        self.assertEqual(self.usernames(self.client.get("/api/users/search/", {"q": "find"})), [self.normal.username])
        profile = self.client.get(f"/api/users/{self.normal.username}/")
        self.assertEqual(profile.status_code, 200)
        self.assertFalse(profile.json()["can_view_full_profile"])
        self.assertEqual(profile.json()["profile_access"], "restricted")
        for owner, blocked in [(self.viewer, self.normal), (self.normal, self.viewer)]:
            block = UserVisibilityBlock.objects.create(owner=owner, blocked_user=blocked)
            self.assertEqual(self.usernames(self.client.get("/api/users/search/", {"q": "find"})), [])
            block.delete()

    def test_search_limit_and_order_apply_after_internal_exclusion(self):
        for index in range(21):
            User.objects.create_user(f"FindAStaff{index:02}", is_staff=True)
            User.objects.create_user(f"FindBNormal{index:02}")
        names = self.usernames(self.client.get("/api/users/search/", {"q": "find"}))
        self.assertEqual(names, [f"FindBNormal{index:02}" for index in range(20)])

    def test_friend_autocomplete_excludes_internal_accounts_and_preserves_restrictions(self):
        for user in [self.normal, *self.internal]:
            self.befriend(user)
        for query in [{}, {"search": "fInD"}]:
            response = self.client.get("/api/friends/", query)
            self.assertEqual(self.usernames(response), [self.normal.username])
            self.assertEqual(response.json()["count"], 1)
        UserVisibilityBlock.objects.create(owner=self.normal, blocked_user=self.viewer)
        self.assertEqual(self.usernames(self.client.get("/api/friends/")), [])
        self.assertEqual(Friendship.objects.count(), 4)

    def test_privacy_candidate_search_excludes_internal_users_but_keeps_existing_blocks(self):
        path = "/api/profile/privacy/blocked-users/"
        self.assertEqual(self.usernames(self.client.get(path, {"q": "find"})), [self.normal.username])
        UserVisibilityBlock.objects.create(owner=self.viewer, blocked_user=self.normal)
        block = UserVisibilityBlock.objects.create(owner=self.viewer, blocked_user=self.staff)
        self.assertEqual(self.usernames(self.client.get(path, {"q": "find"})), [])
        self.assertContains(self.client.get(path), self.staff.username)
        self.assertTrue(UserVisibilityBlock.objects.filter(pk=block.pk).exists())

    def test_existing_friendships_and_follows_are_unchanged_and_still_listed(self):
        for user in [self.normal, *self.internal]:
            self.befriend(user)
            Follow.objects.create(follower=self.viewer, following=user)
        expected = {user.username for user in [self.normal, *self.internal]}
        for path in ["/api/me/following/", "/api/social/following/", "/api/social/friends/", f"/api/users/{self.viewer.username}/friends/", f"/api/users/{self.viewer.username}/following/"]:
            self.assertEqual(set(self.usernames(self.client.get(path))), expected)
        self.assertEqual(Friendship.objects.filter(status=Friendship.STATUS_ACCEPTED).count(), 4)
        self.assertEqual(Follow.objects.filter(follower=self.viewer).count(), 4)

    def test_admin_queryset_login_permissions_and_database_flags_are_unchanged(self):
        before = list(User.objects.values("id", "is_staff", "is_superuser", "is_active", "password"))
        self.client.get("/api/users/search/", {"q": "find"})
        self.client.get("/api/friends/")
        request = RequestFactory().get("/admin/auth/user/")
        request.user = self.both
        user_admin = admin.site._registry[User]
        for user in self.internal:
            self.assertTrue(user_admin.get_queryset(request).filter(pk=user.pk).exists())
            response = APIClient().post("/api/token/", {"username": user.username, "password": "Valid-password-49!"})
            self.assertEqual(response.status_code, 200)
        self.assertTrue(user_admin.has_view_permission(request))
        self.assertEqual(list(User.objects.values("id", "is_staff", "is_superuser", "is_active", "password")), before)
        self.assertTrue(self.client.login(username=self.both.username, password="Valid-password-49!"))
        self.assertEqual(self.client.get("/admin/auth/user/").status_code, 200)
