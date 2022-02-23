import covalent as ct

@ct.electron(backend="slurm")
def test_func() -> str:
    return "graceful_exit"

@ct.lattice
def test_lat() -> str:
    return test_func()

if __name__ == '__main__':
    dispatch_id = ct.dispatch(test_lat)()
    ct.sync(dispatch_id)
