import os
import pathlib
import shutil


project_root = pathlib.Path(__file__).parent.parent.resolve() 
destination = os.path.join(project_root, 'data', 'destination')
stage = os.path.join(project_root, 'data', 'stage')


class SomeStorageLibrary:

    def __init__(self) -> None:
        print('Instantiating storage library...')
        def create_if_not_exist(dir:str) -> None:
            if not os.path.isdir(dir):
                os.mkdir(dir)
        [create_if_not_exist(_) for _ in [destination, stage]]

    def load_csv(self, filename: str) -> None:
        print(f'Loading the following file to storage medium: {filename}')
        shutil.move(filename, destination)
        print('Load completed!')
