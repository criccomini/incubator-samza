import os

test = {
  "deployment_code": os.path.join(os.path.dirname(os.path.abspath(__file__)), "deployment.py"),
  "test_code": [
      os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests/hello-world-test.py")
      ],
  "perf_code": os.path.join(os.path.dirname(os.path.abspath(__file__)), "perf.py"),
  "configs_directory": os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs/")
}
