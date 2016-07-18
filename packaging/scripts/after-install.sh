#!/bin/bash

set -e

INSTALL_DIR=/home/icecp/modules/"<%= name %>"/"<%= version %>"-"<%= iteration %>"
INSTALL_DIR_LINK=/home/icecp/icecp-module-fork

after_install()
{
  echo "Performing post-install steps for <%= name %> version=<%= version %> iteration=<%= iteration %>"
  ln -s $INSTALL_DIR $INSTALL_DIR_LINK
}
after_install
