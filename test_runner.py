"""Runs all unit tests for the SAKA Cloud Function and dependent modules."""
import argparse
import os
import sys
import unittest


def main(test_path, test_pattern):
  # Discover and run tests.
  suite = unittest.loader.TestLoader().discover(test_path, test_pattern)
  return unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument(
      '--test-pattern',
      help='The file pattern for test modules, defaults to *_test.py.',
      default='*_test.py')
  args = parser.parse_args()

  test_paths = [os.getcwd(),
                f'{os.getcwd()}/cloud_functions',
                f'{os.getcwd()}/cloud_functions/lib',
               ]
  test_failed = False
  for path in test_paths:
    result = main(path, args.test_pattern)
    if not result.wasSuccessful():
      test_failed = True
  if test_failed:
    sys.exit(1)
