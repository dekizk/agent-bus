"""The README is also the PyPI project description, where relative links break."""
import re
import unittest
from pathlib import Path

README = Path(__file__).resolve().parents[1] / 'README.md'


class ReadmeLinkTests(unittest.TestCase):
    def test_links_and_images_are_absolute_or_in_page(self):
        targets = re.findall(r'\]\(([^)\s]+)\)', README.read_text(encoding='utf-8'))
        targets += re.findall(r'src="([^"]+)"', README.read_text(encoding='utf-8'))
        self.assertTrue(targets)
        relative = [t for t in targets if not re.match(r'(https://|mailto:|#)', t)]
        self.assertEqual([], relative, 'use absolute GitHub URLs so links work on PyPI')


if __name__ == '__main__':
    unittest.main()
