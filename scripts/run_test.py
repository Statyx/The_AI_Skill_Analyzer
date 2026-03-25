"""Quick wrapper to run analyzer and capture all output to a file.

Usage:
    python run_test.py                          # default: run --serial
    python run_test.py --profile marketing360 run --tag kpi
    python run_test.py rerun 20260324_231048 --questions 3
    python run_test.py analyze --latest --html
"""
import subprocess
import sys
import os

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
out_file = os.path.join(root, "run_output.txt")

extra_args = sys.argv[1:] if len(sys.argv) > 1 else ["run", "--serial"]

# Use the new package: python -m analyzer
cmd = [sys.executable, "-u", "-m", "analyzer"] + extra_args

env = os.environ.copy()
env["PYTHONIOENCODING"] = "utf-8"

label = " ".join(extra_args)
print(f"Running: analyzer.py {label}")
print(f"Output will also be saved to: {out_file}")
print("=" * 60)

# Run with both stdout display and file capture
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        cwd=root, env=env)

with open(out_file, "w", encoding="utf-8") as f:
    for line in proc.stdout:
        text = line.decode("utf-8", errors="replace")
        sys.stdout.write(text)
        sys.stdout.flush()
        f.write(text)
        f.flush()

proc.wait()
print(f"\n{'=' * 60}")
print(f"Done (exit code: {proc.returncode})")
print(f"Output saved to: {out_file}")
