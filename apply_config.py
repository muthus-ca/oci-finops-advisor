"""Apply explicit function config with Fn CLI; no shell interpolation or embedded credentials."""
import argparse
import json
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--app', required=True)
    parser.add_argument('--function', required=True)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    with open(args.config) as stream:
        config = json.load(stream)
    if not isinstance(config, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in config.items()):
        raise SystemExit('Configuration must be an object of string values')
    if any('REPLACE_WITH' in v for v in config.values()):
        raise SystemExit('Fill in every placeholder before applying configuration')
    for key, value in config.items():
        subprocess.run(['fn', 'config', 'function', args.app, args.function, key, value], check=True)
    print('Applied %d configuration values. Verify detached timeout separately.' % len(config))


if __name__ == '__main__':
    main()
