import os

def detect_extension(filename):
    return filename.rsplit(".", 1)[-1].lower()
def make_dirs(path):
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
