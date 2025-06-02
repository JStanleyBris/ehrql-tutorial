FROM mcr.microsoft.com/devcontainers/python:3.11-bullseye

# Suppress the codespace's default first run message
# See https://github.com/devcontainers/features/blob/main/src/common-utils/scripts/rc_snippet.sh
RUN mkdir -p /home/vscode/.config/vscode-dev-containers
RUN touch /home/vscode/.config/vscode-dev-containers/first-run-notice-already-displayed

# Set the default message for every new terminal
COPY motd /etc/motd
RUN echo "cat /etc/motd" >> /home/vscode/.bashrc
