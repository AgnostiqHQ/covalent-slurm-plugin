# Testing with a Slurm docker container

## Prerequisites

Ensure you have docker installed and running on your system. You can check this by running `docker ps` and ensuring that you get a list of running containers.

We will be using the slurm docker image from [here](https://hub.docker.com/r/turuncu/slurm). You can pull this image by running:

```bash
docker pull turuncu/slurm
```

Also make sure your current directory is `tests/docker_tests` for all the commands mentioned below.

## Building the image

### Generating a keypair

We need to generate a keypair to allow the executor to ssh into the container. To do this run:

```bash
ssh-keygen -t ed25519 -f slurm_test -N ''
```

This will generate a keypair called `slurm_test` and `slurm_test.pub` in the current directory. We will use these keys to allow the executor to ssh into the container.

The name of the key is important as it is used in the `Dockerfile` to copy the public key into the container.

### Running docker build

We do some additional setup to the image so that the executor is able to ssh into the container. To do this make sure you are in the right directory (tests/docker_tests) and run:

```bash
docker build -t slurm-image .
```

This will build the image and tag it as `slurm-image`.

## Running the container

To run the container, run:

```bash
docker run -d -p 22:22 --name slurm-container slurm-image
```

This will run the container in the background with name `slurm-container` and map port 22 of the container to port 22 of the host machine. This will allow us to ssh into the container.

### Changing the permissions of the slurm config

We need to change the permissions of the slurm config file so that `slurmuser` can read it. To do this run:

```bash
docker exec slurm-container chmod +r /etc/slurm/slurm.conf
```

## (Optional) Try running a basic slurm job

To test that the container is working, we can try running a basic slurm job. To do this, ssh into the container by running:

```bash
ssh -i slurm_test slurmuser@localhost
```

Then inside the container, run:

```bash
sbatch test.job
```

This will submit the test job to the slurm scheduler and create two new files in the current directory (should be `/home/slurmuser`) as `test_<job-id>.out` and `test_<job-id>.err`. The `.out` file should contain the stdout output of the job (should be "Hello World") and the `.err` file should contain any errors (should contain the python version as it is redirected to stderr).

## Running the tests

Now that we have everything set up, use your favourite workflow and assign the executor as `@ct.electron(executor=slurm_executor)` for any of the electrons, where the `slurm_executor` is defined as:

```python
from covalent_slurm_plugin import SlurmExecutor

slurm_executor = SlurmExecutor(username="slurmuser", address="localhost", ssh_key_file="./slurm_test", conda_env="covalent", ignore_versions=True)
```

You can mark `ignore_versions` as `False` (which is the default) if you want to make sure the same versions of python, covalent, and cloudpickle are used in the slurm job as on your local machine.
