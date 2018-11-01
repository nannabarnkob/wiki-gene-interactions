#!/bin/sh
awk ' {
if ($1 == 9606)
  print $0
}
'
