from test import TestCase


class UBPhase1VREM(TestCase):
    def getname(self):
        return "UB Phase 1 VREM semantics"

    def test(self):
        key = self.test_key + ":vrem"

        self.redis.execute_command('VADD', key, 'VALUES', 4, '1', '0', '0', '0', key + ':item:1')
        self.redis.execute_command('VADD', key, 'VALUES', 4, '0', '1', '0', '0', key + ':item:2')

        before_card = self.redis.execute_command('VCARD', key)
        assert before_card == 2, f"unexpected cardinality before delete: {before_card}"

        rem = self.redis.execute_command('VREM', key, key + ':item:1')
        assert rem == 1, f"expected VREM to delete existing item, got {rem}"

        emb = self.redis.execute_command('VEMB', key, key + ':item:1')
        assert emb is None, "deleted element should return nil in VEMB"

        after_card = self.redis.execute_command('VCARD', key)
        assert after_card == 1, f"unexpected cardinality after delete: {after_card}"

        vsim = self.redis.execute_command(
            'VSIM', key, 'VALUES', 4, '1', '0', '0', '0', 'WITHSCORES', 'COUNT', 2
        )
        assert key + ':item:1' not in vsim, f"deleted element should not appear in VSIM: {vsim}"
        assert key + ':item:2' in vsim, f"remaining element missing from VSIM: {vsim}"
