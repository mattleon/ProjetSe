#include "kv.h"


int main() {
  KV *kv = kv_open("test", "r+",1, FIRST_FIT);
  kv_close(kv);
}
