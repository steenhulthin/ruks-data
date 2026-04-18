# TODO

- Investigate suppression / missing-value rules in the RUKS workbook and downstream publication.
- Confirm whether blank cells in `Hovedresultater` are due to disclosure control / diskretionering rather than true zeros or unavailable source data.
- Decide how suppressed values should be represented in outputs: `NULL`, explicit suppression flag, or a display label for website use.
- Check whether the relevant explanation can be extracted from the workbook, or whether it should be documented from external Sundhedsdatastyrelsen guidance.
- Add semantic smoke tests for expected content: 9 diseases, 5 regions, 98 municipalities, and expected measure/unit combinations.
- Short-circuit the workflow when the downloaded workbook SHA-256 has not changed, to avoid unnecessary commits and releases.
- Add a small front-page chart or table beyond the current summary card, using the generated `latest-summary.json`.
- Add one or two more usage examples, for example municipality-level filtering and a region comparison.
- Decide whether `Dokumentation` and `Opmærksomhedspunkter` should remain prose documentation only or be captured in structured machine-readable form.
