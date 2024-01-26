# Copyright 2024 Agnostiq Inc.
#
# This file is part of Covalent.
#
# Licensed under the Apache License 2.0 (the "License"). A copy of the
# License may be obtained with this software package or at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Use of this file is prohibited except in compliance with the License.
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


FROM turuncu/slurm:latest

RUN apt update && apt install openssh-server vim less wget sudo -y

# Create a user “slurmuser” and group “slurmgroup”
RUN groupadd slurmgroup && useradd -ms /bin/bash -g slurmgroup slurmuser
RUN echo 'slurmuser:root123' | chpasswd

# Create slurmuser directory in home
RUN mkdir -p /home/slurmuser/.ssh

# Copy the ssh public key in the authorized_keys file. The idkey.pub below is a public key file you get from ssh-keygen. They are under ~/.ssh directory by default.
COPY slurm_test.pub /home/slurmuser/.ssh/authorized_keys

# Copy the test.job file to the home directory
COPY test.job /home/slurmuser/test.job

# change ownership of the key file.
RUN chown slurmuser:slurmgroup /home/slurmuser/.ssh/authorized_keys && chmod 600 /home/slurmuser/.ssh/authorized_keys

# Start SSH service
RUN service ssh start

# Export home as /home/slurmuser
ENV HOME /home/slurmuser

# Remove the .bashrc conditional for non-interactive shells
RUN sed -e '/case $-/,+3 s/^/#/' -i /home/slurmuser/.bashrc

# Change directory to /home/slurmuser
WORKDIR /home/slurmuser

# Miniconda installation
RUN MINICONDA_SH=Miniconda3-latest-Linux-x86_64.sh && \
    wget https://repo.anaconda.com/miniconda/$MINICONDA_SH && \
    bash $MINICONDA_SH -b -p $HOME/miniconda3 && \
    rm $MINICONDA_SH

# Add conda to path
ENV PATH /home/slurmuser/miniconda3/bin:$PATH

# Conda init
RUN eval "$(conda shell.bash hook)" && \
    conda init bash

# Create the conda environment
RUN conda create -n covalent python=3.10 -y

# Add conda env activation to bashrc
RUN echo "conda activate covalent" >> ${HOME}/.bashrc

# Make slurmuser the owner of the conda environment and cache
RUN chown -R slurmuser:slurmgroup /home/slurmuser/miniconda3 && \
    chown -R slurmuser:slurmgroup /home/slurmuser/.conda && \
    chown -R slurmuser:slurmgroup /home/slurmuser/.cache

# Install covalent
RUN conda run -n covalent python -m pip install covalent


# Expose docker port 22
EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]
