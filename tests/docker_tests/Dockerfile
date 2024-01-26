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

# Copy covalent install file to the home directory
COPY covalent_install.sh /home/slurmuser/covalent_install.sh

# Run the covalent install file
RUN chmod +x /home/slurmuser/covalent_install.sh && /home/slurmuser/covalent_install.sh

# change ownership of the key file.
RUN chown slurmuser:slurmgroup /home/slurmuser/.ssh/authorized_keys && chmod 600 /home/slurmuser/.ssh/authorized_keys

# Start SSH service
RUN service ssh start

# Expose docker port 22
EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]
