FROM astrocrpublic.azurecr.io/runtime:3.0-2

# Install latest Azure CLI using Microsoft's official method
USER root
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash
USER astro