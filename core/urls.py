from django.urls import path
from .views import (
    UserProfileView, MeView, FollowToggleView,UserFollowersListView, UserFollowingListView,
    FeedFollowingView, DiscoverView, PostListCreateView, PostDetailView, PostRatingView,
    UserPostsListView, PostCommentsListCreateView, CommentDetailView, RegisterView,
)

urlpatterns = [
    path("register/", RegisterView.as_view(), name="register"),
    path("users/<str:username>/", UserProfileView.as_view(), name="user-profile"),
    path("users/<str:username>/posts/", UserPostsListView.as_view(), name="user-posts"),
    path("me/", MeView.as_view(), name="me"), 
    path("follow/<str:username>/", FollowToggleView.as_view(), name="follow-toggle"),
    path("users/<str:username>/followers/", UserFollowersListView.as_view(), name="user-followers"),
    path("users/<str:username>/following/", UserFollowingListView.as_view(), name="user-following"),
    path("feed/", FeedFollowingView.as_view(), name="feed-following"),
    path("discover/", DiscoverView.as_view(), name="discover"),
    path("posts/", PostListCreateView.as_view(), name="post-list-create"),
    path("posts/<int:pk>/", PostDetailView.as_view(), name="post-detail"),
    path("posts/<int:pk>/rating/", PostRatingView.as_view(), name="post-rating"),
    path("posts/<int:pk>/comments/", PostCommentsListCreateView.as_view(), name="post-comments"),
    path("comments/<int:pk>/", CommentDetailView.as_view(), name="comment-detail"),
]
