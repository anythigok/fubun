# ADR-0022: Session順序にreceived_atを使う

Status: accepted

Daemonが受信した`received_at`をSession順序の正本にし、同時刻はEvent IDで安定化する。
Adapterが指定する`occurred_at`は表示用の意味に留め、再送や時計ずれでSession順序を
変えない。
