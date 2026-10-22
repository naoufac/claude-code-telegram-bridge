import os
import queue
import tempfile
import unittest
from unittest.mock import patch

os.environ.setdefault('TG_TOKEN', 'offline-test')
os.environ.setdefault('OWNER_ID', '42')
import bridge as b


class CommandTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for name, value in [('STATE_PATH', self.tmp.name + '/state.json'),
                            ('STATE', {'sessions': {}, 'offset': 0}),
                            ('work_q', queue.Queue()), ('current_proc', None),
                            ('MODEL', ''), ('EFFORT', ''), ('RUN_TIMEOUT', 0)]:
            p = patch.object(b, name, value)
            p.start()
            self.addCleanup(p.stop)
        self.sent = []
        p = patch.object(b, 'send_text', side_effect=lambda chat, text: self.sent.append(text))
        p.start()
        self.addCleanup(p.stop)
        p = patch.object(b, 'tg', return_value={'ok': True})
        p.start()
        self.addCleanup(p.stop)

    def say(self, text):
        self.sent.clear()
        b.handle_message({'chat': {'id': 42, 'type': 'private'}, 'from': {'id': b.OWNER}, 'text': text})
        return '\n'.join(self.sent)

    def test_menu_has_thirty_unique_commands(self):
        self.assertEqual(len(b.MENU), 30)
        self.assertEqual(len({c['command'] for c in b.MENU}), 30)

    def test_info_commands_reply_and_never_queue(self):
        if True:
            for cmd in ('help', 'start', 'models', 'model', 'effort', 'timeout', 'status', 'ping', 'workspace',
                        'queue', 'cost', 'usage', 'session', 'health', 'uptime', 'config', 'log', 'sessions'):
                out = self.say('/' + cmd + '@example_bot')
                self.assertTrue(out, cmd)
                self.assertEqual(b.work_q.qsize(), 0, cmd)

    def test_model_and_effort_persist_and_reach_the_cli(self):
        self.say('/model sonnet')
        self.assertEqual(b.MODEL, 'sonnet')
        self.say('/effort max')
        self.assertEqual(b.EFFORT, 'max')
        self.assertEqual(b.STATE['primary_model'], 'sonnet')
        self.assertIn('Unknown model', self.say('/model banana'))
        self.assertIn('GLM worker', self.say('/model glm-5.3'))
        self.say('/model default')
        self.say('/effort default')
        self.assertEqual((b.MODEL, b.EFFORT), ('', ''))

    def test_timeout_zero_means_no_limit(self):
        self.assertIn('15 minutes', self.say('/timeout 15'))
        self.assertEqual(b.RUN_TIMEOUT, 900)
        self.assertIn('none', self.say('/timeout 0'))
        self.assertEqual(b.RUN_TIMEOUT, 0)

    def test_queueing_commands(self):
        self.say('/zai summarize this')
        self.assertEqual(b.work_q.get_nowait()[2], 'zai')
        b.STATE['last_prompt'] = {'42': 'do the thing'}
        self.say('/retry')
        self.assertEqual(b.work_q.get_nowait()[1:], ('do the thing', 'claude'))
        self.say('hello')
        self.say('again')
        self.assertIn('2', self.say('/clear'))
        self.assertEqual(b.work_q.qsize(), 0)

    def test_goal_and_unknown_commands_pass_through_to_claude(self):
        self.say('/goal finish the site')
        self.assertEqual(b.work_q.get_nowait()[1], '/goal finish the site')
        self.say('/copywriting a headline')
        self.assertEqual(b.work_q.get_nowait()[1], '/copywriting a headline')

    def test_cost_tracking_reported(self):
        b.STATE['last_cost'] = {'usd': 0.1234, 'ts': b.time.time(), 'model': 'account default', 'effort': 'max'}
        b.STATE['cost_total'] = 1.5
        self.assertIn('0.1234', self.say('/cost'))
        self.assertIn('1.5000', self.say('/usage'))


if __name__ == '__main__':
    unittest.main()
