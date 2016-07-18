#!/bin/bash

set -e

INSTALL_DIR=/home/icecp/modules/"<%= name %>"/"<%= version %>"-"<%= iteration %>"
INSTALL_DIR_LINK=/home/icecp/icecp-module-fork

after_remove()
{
  echo "Performing post-removal steps for <%= name %> version=<%= version %> iteration=<%= iteration %>"
  
  # Remove soft link and icecp-node directory contents
  if [ -L $INSTALL_DIR_LINK ] && [ "$(readlink $INSTALL_DIR_LINK)" = $INSTALL_DIR ]; then
    rm $INSTALL_DIR_LINK
    echo "Removed soft link $INSTALL_DIR_LINK"
  fi

  if [ -d $INSTALL_DIR ]; then
    rm -rf $INSTALL_DIR
    echo "Removed $INSTALL_DIR"
  fi
}

after_remove