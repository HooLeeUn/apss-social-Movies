import json

from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory

from core.views import LEGAL_POLICIES_PAYLOAD, LegalPoliciesView


class LegalPoliciesTextTests(SimpleTestCase):
    def test_public_payload_has_current_date_and_no_imdb(self):
        response = LegalPoliciesView.as_view()(APIRequestFactory().get('/legal/policies/'))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['last_updated'], '2026-10-16')
        self.assertNotIn('imdb', json.dumps(response.data, ensure_ascii=False).lower())

    def test_rating_policy_describes_progressive_mix_and_inclusive_threshold(self):
        section = LEGAL_POLICIES_PAYLOAD['sections'][1]
        self.assertEqual(section['subtitle'], 'Sistema de calificaciones de RecCool')
        self.assertEqual(len(section['content']), 6)
        text = ' '.join(section['content'])
        for phrase in ('menos de 100', 'TMDB', 'aumenta progresivamente', 'Al alcanzar 100',
                       'utiliza exclusivamente el promedio', 'no necesariamente se sincronizan en tiempo real'):
            self.assertIn(phrase, text)
        for phrase in ('5000', 'reducción de peso', 'volátiles', 'manipulables', 'supera 100'):
            self.assertNotIn(phrase, text)

    def test_external_accuracy_and_ownership_use_requested_wording(self):
        accuracy = next(s for s in LEGAL_POLICIES_PAYLOAD['sections'] if s['subtitle'] == 'Exactitud de datos externos')
        self.assertEqual(accuracy['content'][-1], 'RecCool podrá corregir, actualizar u ocultar información cuando sea necesario.')
        self.assertNotIn('ajustar', ' '.join(accuracy['content']))
        ownership = LEGAL_POLICIES_PAYLOAD['intellectual_property_sections'][0]['content']
        self.assertEqual(ownership[2], 'Los datos, imágenes y demás contenidos provenientes de fuentes externas pertenecen a sus respectivos titulares y se utilizan de acuerdo con las condiciones aplicables a dichas fuentes.')

    def test_credits_supply_renderable_content_and_exact_attribution(self):
        credits = LEGAL_POLICIES_PAYLOAD['credits']
        attribution = 'This product uses the TMDB API but is not endorsed or certified by TMDB.'
        self.assertEqual(credits['content'][0], attribution)
        self.assertEqual(credits['tmdb']['attribution'], attribution)
        self.assertEqual(len(credits['content']), 5)
        self.assertIn('JustWatch', credits['content'][-1])
        self.assertIn('no necesariamente se encuentra sincronizada en tiempo real', credits['content'][2])
