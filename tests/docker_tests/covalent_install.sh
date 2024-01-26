#!/bin/bash

set -eu -o pipefail
export HOME=/home/slurmuser

sed -i '/^case \\$-.*/,+3d' /home/slurmuser/.bashrc
cd $HOME

MINICONDA_EXE="Miniconda3-py38_23.3.1-0-Linux-x86_64.sh"
wget https://repo.anaconda.com/miniconda/$MINICONDA_EXE
chmod +x $MINICONDA_EXE
./$MINICONDA_EXE -b -p $HOME/miniconda3
rm $MINICONDA_EXE

export PATH=$HOME/miniconda3/bin:$PATH
eval "$(conda shell.bash hook)"
conda init bash

conda create -n covalent python=3.10 -y
echo "conda activate covalent" >> $HOME/.bashrc

chown -R slurmuser:slurmgroup $HOME/{.cache,.conda,miniconda3}
conda run -n covalent python -m pip install covalent
