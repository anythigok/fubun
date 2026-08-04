# ADR-0023: 汎用系列マイニングではなく決定論的Prefixを使う

Status: accepted

各WorkspaceのSession先頭から連続する2〜5 ResourceだけをCandidateにする。非連続
n-gram、順序入替、PrefixSpan、機械学習は実装しない。最長の適格Prefixを決定論的に一つ
だけ残す。
