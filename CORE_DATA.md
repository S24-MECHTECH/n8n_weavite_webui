# CORE DATEN REGELN

## CRITICAL: Niemals löschen!

Diese Daten sind **Core-Daten** und dürfen **NIEMALS gelöscht** werden!

### Was sind Core-Daten?
- Geschäftsregeln und Gesetze (HGB, GoBD, Steuerrecht)
- Buchhaltungswissen
- Kunden- und Lieferantendaten
- Buchungssätze
- Rechnungen (Eingang/Ausgang)
- Bankdaten
- Steuerextraktionen

### betroffene Weaviate-Klassen (NIEMALS LÖSCHEN):

#### Buchhaltung Core
- `Lexware_rag_knowledge` - RAG Wissen (Regeln, Gesetze)
- `Lexware_Buchhaltung` - Haupt-Buchhaltung
- `Buchhaltung_Master_Brain` - Master-Wissen
- `Buchungssaetze` / `Lexware_buchungssaetze` - Buchungssätze
- `Buchungenfinal` / `Lexware_buchungen_final` - Finale Buchungen

#### Rechnungen
- `Eingangsrechnungen` / `Lexware_eingangsrechnungen`
- `Ausgangsrechnungen` / `Lexware_ausgangsrechnung`

#### Finanzen & Bank
- `BankkontoKnowledge` / `Lxw_bankkonto_knowledge`
- `KreditkartenkontoKnowledge` / `Lxw_kreditkartenkonto_knowledge`
- `KassenkontoKnowledge` / `Lxw_kassenkonto_knowledge`
- `Finanzamt_Schriftverkehr` / `Finanzamt_Steuerbescheide`

#### Steuer
- `Lexware_steuer_extraktion` - Steuer-Extraktionen
- `Accountingknowledge` / `Lxw_accounting_knowledge`

### Backup-Regeln:
1. **Vor jeder Änderung** Backup erstellen:
   ```bash
   python3 backup_weaviate.py
   ```
2. **Regelmäßig** Backups prüfen:
   ```bash
   python3 backup_weaviate.py --list
   ```
3. **Vor Delete All** IMMER erst Backup!

### Restore bei versehentlichem Löschen:
```bash
python3 backup_weaviate.py --restore /path/to/backup.zip
```

### Agent-Logs (NICHT löschen - Audit-Trail!)
- `Agent_action_log` / `Agent_actions`
- `Agent_chain_of_custody`
- `Agent_governance_log`
- `Agent_audit_log`
- `Agent_revision_log`

## WICHTIGE VERBOTE:
❌ DELETE ALL in der WebUI = VERBOTEN ohne Backup
❌ Direktes Löschen von Core-Klassen = VERBOTEN
❌ Überschreiben von Core-Daten = NUR mit Bestätigung
