from utils import KineticSwiftTestCase


class TestKineticSwiftClient(KineticSwiftTestCase):

    def setUp(self):
        super(TestKineticSwiftClient, self).setUp()
        self.client = self.client_map[self.PORTS[0]]

    def test_iter_keys(self):
        for i in range(13):
            key = 'objects.asdf.%03d' % i
            self.client.put(key, '')

        self.assertEqual([
            'objects.asdf.000',
            'objects.asdf.001',
            'objects.asdf.002',
            'objects.asdf.003',
        ], list(self.client.iterKeyRange(
            'objects.', 'objects/', maxReturned=2))[:4])

        self.assertEqual([
            'objects.asdf.009',
            'objects.asdf.010',
            'objects.asdf.011',
            'objects.asdf.012',
        ], list(self.client.iterKeyRange(
            'objects.', 'objects/', maxReturned=2))[-4:])

    def test_iter_keys_reversed(self):
        for i in range(13):
            key = 'objects.asdf.%03d' % i
            self.client.put(key, '')

        self.assertEqual([
            'objects.asdf.012',
            'objects.asdf.011',
            'objects.asdf.010',
            'objects.asdf.009',
        ], list(self.client.iterKeyRange(
            'objects.', 'objects/', maxReturned=2, reverse=True))[:4])

        self.assertEqual([
            'objects.asdf.003',
            'objects.asdf.002',
            'objects.asdf.001',
            'objects.asdf.000',
        ], list(self.client.iterKeyRange(
            'objects.', 'objects/', maxReturned=2, reverse=True))[-4:])
