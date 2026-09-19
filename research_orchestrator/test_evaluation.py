"""Acceptance-harness checks; synthetic responses must never supply evidence."""
import unittest
from .evaluate import score_case


class ContractTests(unittest.TestCase):
    def test_target_cannot_be_dropped(self):
        self.assertEqual(score_case({'expect':{'objectives.cagr':30}},{'spec':{'objectives':{'cagr':0}}})[0]['check'],'objectives.cagr')

    def test_boolean_meaning_cannot_change(self):
        self.assertTrue(score_case({'expect':{'join':'any'}},{'spec':{'join':'all'}}))

    def test_followup_must_retain_existing_condition(self):
        case={'conditions':[{'kind':'rsi_below','value':25},{'kind':'volume','value':1}]}
        self.assertTrue(score_case(case,{'spec':{'conditions':[{'kind':'volume','value':1}]}}))

    def test_matching_contract_is_accepted(self):
        self.assertFalse(score_case({'expect':{'capital':1000000,'action':'research'}},{'action':'research','spec':{'capital':1000000}}))

    def test_unsupported_requirement_needs_explicit_resolution(self):
        self.assertTrue(score_case({'requires_clarification':True},{'notes':['Using a different assumption']}))
        self.assertFalse(score_case({'requires_clarification':True},{'blockers':['Sector constraint needs implementation']}))


if __name__=='__main__':unittest.main()
