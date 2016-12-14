#!/bin/bash

wget -qO- "http://www.wowhead.com/item=$1" | grep '</title>' | sed -r 's/.*>(.*) - Item .*/\1/'
