"""Download competition files without embedding Kaggle credentials."""
import argparse
from pathlib import Path
import kagglehub

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='data/raw')
    args = parser.parse_args()
    path = kagglehub.competition_download('h-and-m-personalized-fashion-recommendations')
    import shutil
    destination = Path(args.output).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    shutil.copytree(path, destination, dirs_exist_ok=True)
    print(f'Dataset ready: {destination}')

if __name__ == '__main__':
    main()
