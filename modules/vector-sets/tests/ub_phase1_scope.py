from test import TestCase


class UBPhase1Scope(TestCase):
    def getname(self):
        return "UB Phase 1 key-scoped VADD/VEMB/VSIM"

    def test(self):
        key_a = self.test_key + ":a"
        key_b = self.test_key + ":b"

        vec_a1 = ['1', '0', '0', '0']
        vec_a2 = ['0.9', '0.1', '0', '0']
        vec_b1 = ['0', '1', '0', '0']

        self.redis.execute_command('VADD', key_a, 'VALUES', 4, *vec_a1, key_a + ':item:1')
        self.redis.execute_command('VADD', key_a, 'VALUES', 4, *vec_a2, key_a + ':item:2')
        self.redis.execute_command('VADD', key_b, 'VALUES', 4, *vec_b1, key_b + ':item:1')

        emb_a = [float(x) for x in self.redis.execute_command('VEMB', key_a, key_a + ':item:1')]
        emb_b = [float(x) for x in self.redis.execute_command('VEMB', key_b, key_b + ':item:1')]

        assert abs(emb_a[0] - 1.0) < 1e-6, f"unexpected key_a result: {emb_a}"
        assert abs(emb_b[1] - 1.0) < 1e-6, f"unexpected key_b result: {emb_b}"

        cross = self.redis.execute_command('VEMB', key_b, key_a + ':item:1')
        assert cross is None, "cross-key VEMB should return nil"

        vsim_a = self.redis.execute_command(
            'VSIM', key_a, 'VALUES', 4, '1', '0', '0', '0', 'WITHSCORES', 'COUNT', 2
        )
        vsim_b = self.redis.execute_command(
            'VSIM', key_b, 'VALUES', 4, '1', '0', '0', '0', 'WITHSCORES', 'COUNT', 2
        )

        assert vsim_a[0] == key_a + ':item:1', f"unexpected VSIM key_a result: {vsim_a}"
        assert key_b + ':item:1' in vsim_b, f"unexpected VSIM key_b result: {vsim_b}"

        card_a = self.redis.execute_command('VCARD', key_a)
        card_b = self.redis.execute_command('VCARD', key_b)
        dim_a = self.redis.execute_command('VDIM', key_a)
        dim_b = self.redis.execute_command('VDIM', key_b)

        assert card_a == 2, f"unexpected key_a cardinality: {card_a}"
        assert card_b == 1, f"unexpected key_b cardinality: {card_b}"
        assert dim_a == 4, f"unexpected key_a dimension: {dim_a}"
        assert dim_b == 4, f"unexpected key_b dimension: {dim_b}"
