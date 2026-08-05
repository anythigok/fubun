# ADR-0021: Phase 4A DiscoveryをWorkspace Startへ限定する

Status: accepted

Phase 4Aは`workspace-browser-start/v1`だけを扱う。VS Code Workspace OpenedをAnchor、
Browser Resource OpenedをAction Observationとし、他のDiscovery Kind、Generic Miner、
Rule、Schedulerは後続Phaseに残す。対象を限定することでEvidenceの意味とSecurity境界を
検証可能にする。
