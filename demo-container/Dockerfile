FROM fedora:latest
RUN dnf -y update && \
dnf -q -y install java-11-openjdk && \
dnf -q -y install dotnet-sdk-3.1 dotnet-runtime-3.1 && \
dnf -q -y install unzip wget git && \
dnf clean all && rm -rf /var/cache/dnf
