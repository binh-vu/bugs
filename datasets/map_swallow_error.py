# import ray hangs the process when error happens
# import ray
from datasets import Dataset

def transformation(ex):
    assert ex['a'] > ex['b']
    return {'c': ex['a'] + ex['b']}

def get_dataset():
    ds: Dataset = Dataset.from_dict({"a": list(range(100)), "b": list(range(100))})
    ds = ds.map(transformation, num_proc=4)
    for ex in ds:
        print(ex)

if __name__ == "__main__":
    get_dataset()