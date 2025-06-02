from ehrql import create_dataset, codelist_from_csv, show
from ehrql.tables.core import patients, practice_registrations, clinical_events, medications

dataset = create_dataset()

index_date = "2024-03-31"

aged_17_or_older = patients.age_on(index_date) >= 17

is_alive = patients.is_alive_on(index_date)

is_registered = practice_registrations.where(
    practice_registrations.start_date <= index_date
).except_where(
    practice_registrations.end_date < index_date
).exists_for_patient()

diabetes_codes = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-dm_cod.csv", column = "code")
diabetes_resolved_codes = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-dmres_cod.csv", column = "code")


last_diagnosis_date = (
    clinical_events.where(clinical_events.snomedct_code.is_in(diabetes_codes))
    .sort_by(clinical_events.date)
    .where(clinical_events.date <= index_date)
    .last_for_patient()
)

last_resolved_date = (
    clinical_events.where(clinical_events.snomedct_code.is_in(diabetes_resolved_codes))
    .sort_by(clinical_events.date)
    .where(clinical_events.date <= index_date)
    .last_for_patient()
)

has_diabetes = clinical_events.where(
    clinical_events.snomedct_code.is_in(diabetes_codes)
    ).exists_for_patient()

has_unresolved_diabetes = last_diagnosis_date.is_not_null() & (
    last_resolved_date.is_null() |
    last_resolved_date < last_diagnosis_date)

on_register = aged_17_or_older & is_alive & is_registered & has_unresolved_diabetes

dataset.define_population(on_register)