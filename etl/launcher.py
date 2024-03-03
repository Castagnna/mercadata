import os
import sys
import importlib

# Adding "/mercadata/etl/.." to sys.path
if (
    not any("/mercadata/etl/.." in p for p in sys.path)
    and "__file__" in vars()
):
    path = os.path.join(os.path.dirname(__file__), os.pardir)
    sys.path.append(path)


def _parse_args():
    from argparse import ArgumentParser, RawDescriptionHelpFormatter
    from textwrap import dedent

    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        epilog=dedent("""Pyspark job arguments."""),
    )

    parser.add_argument(
        "job_name",
        help="The name of the spark job you want to run",
    )
    parser.add_argument("-l", "--layer", help="datalake layer", default="bronze")
    parser.add_argument("-e", "--env", help="environment", default="prd")
    parser.add_argument("-m", "--mode", help="mode", default="cluster")
    parser.add_argument("-d", "--datetime", help="reference datetime", default="today")
    parser.add_argument(
        "--dry-run",
        help="just map input and output paths",
        action="store_true",
        default=False,
    )

    return parser.parse_args()


if __name__ == "__main__":

    args = _parse_args()

    job_algorithm = args.job_name
    if len(job_algorithm) == 1:
        job_algorithm.append("algorithm")

    print("JOB: ", job_algorithm)

    module = "etl.jobs.{}.{}.runner".format(args.layer, args.job_name)
    job_module = importlib.import_module(module)
    job_module.setup(
        args.env, args.datetime, args.mode, args.dry_run
    )
