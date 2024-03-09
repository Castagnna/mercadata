import os
import sys
import importlib

# Adding "/mercadata/etl/.." to sys.path
if not any("/mercadata/etl/.." in p for p in sys.path):
    path = os.path.join(os.getcwd(), os.pardir)
    sys.path.append(path)


def _parse_args():
    from argparse import ArgumentParser, RawDescriptionHelpFormatter
    from textwrap import dedent

    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        epilog=dedent("""Pyspark job arguments."""),
    )

    parser.add_argument(
        "layer",
        help="The name of datalake layer job you want to run",
    )

    parser.add_argument(
        "job_name",
        help="The name of the spark job you want to run",
    )

    parser.add_argument("-e", "--env", help="environment", default="prd")
    parser.add_argument("-m", "--mode", help="mode", default="cluster")
    parser.add_argument("-d", "--datetime", help="reference datetime", default="today")
    parser.add_argument(
        "--dry-run",
        help="just map input and output paths",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-n",
        "--noop",
        help="set spark write format to noop",
        action="store_true",
        default=False,
    )
    parser.add_argument("--job-kwargs", help="job arguments", required=False, nargs="*")

    return parser.parse_args()


if __name__ == "__main__":

    args = _parse_args()

    job_kwargs = dict([x.split("=") for x in (args.job_kwargs or [])])

    app_name = "{}.{}.{}".format(args.layer, args.job_name, args.env)
    module = "etl.jobs.{}.{}.runner_v2".format(args.layer, args.job_name)
    print(f"{module = }")
    job_module = importlib.import_module(module)

    job_module.Setup(
        args.env,
        args.datetime,
        app_name,
        args.mode,
        args.dry_run,
        args.noop,
        **job_kwargs,
    ).run()
