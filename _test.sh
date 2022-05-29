docker run -it --rm \
  -v $(pwd)/cfg:/app/cfg \
  -v $(pwd)/result:/app/result \
  -v $(pwd)/log:/app/log \
  vadimkozin/bill /bin/bash

#docker run -it --rm  vadimkozin/bill /bin/bash
