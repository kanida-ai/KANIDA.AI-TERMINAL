import unittest

from .numerical_probe import assess, select, verify_account


def evidence(trades=10, cagr=10, drawdown=10, return_pct=10):
    return dict(metrics=dict(trades=trades,cagr=cagr,max_drawdown_pct=drawdown,return_pct=return_pct))


class NumericalContractTests(unittest.TestCase):
    def test_high_return_with_too_few_training_trades_is_not_selected(self):
        self.assertEqual(select(dict(original=evidence(),few=evidence(trades=1,cagr=100))), 'original')

    def test_no_eligible_training_candidate_returns_none(self):
        self.assertIsNone(select(dict(original=evidence(trades=2))))

    def test_sparse_winning_test_is_not_promoted(self):
        result=assess(dict(original=evidence(return_pct=-10),selected=evidence(trades=1,return_pct=20)),'selected')
        self.assertEqual(result['status'],'insufficient_test_evidence')
        self.assertFalse(result['promotion_allowed'])

    def test_large_test_still_requires_final_validation(self):
        result=assess(dict(original=evidence(),selected=evidence(trades=100,return_pct=20)),'selected')
        self.assertEqual(result['status'],'not_validated')
        self.assertFalse(result['promotion_allowed'])

    def test_accounting_check_catches_invented_profit(self):
        account=dict(summary=dict(open_positions=0,ending_equity=10001,total_costs=0),trades=[],daily_curve=[])
        with self.assertRaisesRegex(AssertionError,'reconcile'):
            verify_account(account,dict(capital=10000,max_positions=1))


if __name__=='__main__':unittest.main()
