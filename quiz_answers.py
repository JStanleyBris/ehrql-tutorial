#########################################################################################
#                                                                                       #
#              IF YOU'RE LOOKING FOR THE EHRQL QUIZ THIS IS THE WRONG FILE              #
#                                                                                       #
#########################################################################################
#
# The file you want is called `quiz.py`.
#
# This file defines the ehrQL quiz questions and provides the answers. If you don't want
# to spoil the quiz, don't look ahead!
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

from ehrql import codelist_from_csv, show, months
from ehrql.quiz import Question, Questions
from ehrql.tables.core import clinical_events


introduction = """\
Welcome to the ehrQL Quiz!
"""

diabetes_codes = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-dm_cod.csv", column="code"
)
referral_codes = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-dsep_cod.csv", column="code"
)
mild_frailty_codes = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-mildfrail_cod.csv", column="code"
)
moderate_frailty_codes = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-modfrail_cod.csv", column="code"
)
severe_frailty_codes = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-sevfrail_cod.csv", column="code"
)
hba1c_codes = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-ifcchbam_cod.csv", column="code"
)


questions = Questions()
questions.set_dummy_tables_path("dummy_tables")

#
#
#
#
#
#
#
#
# Common help messages

remember_brackets = (
    "\n\nRemember your brackets. Things like this:\n\n"
    "  has_diagnosis & latest_value < 58\n\n"
    "won't work because the '&' gets evaluated first, so it's as if you'd written:\n\n"
    "  (has_diagnosis & latest_value) < 58\n\n"
    "which doesn't make sense and will lead to an error. Instead you need to add brackets "
    "to make the order of the operations explicit:\n\n"
    "  has_diagnosis & (latest_value < 58)"
)


remember_to_sort = (
    "\n\nThe functions first_for_patient() and last_for_patient() can't be used directly "
    "on a table (such as clinical_events) unless you have first called "
    'sort_by(clinical_events.date) so we know what "first" and "last" correspond to. '
    "You could, for example, sort by numeric_value instead, and then first/"
    "last_for_patient would return the lowest/highest value instead of the first/last date."
)

boolean_series_none_to_false = (
    '\n\nIf you get errors like "expected False, got None instead", then what is happening '
    "is that there are patients who don't have the particular thing of interest and so one "
    "of your queries is returning empty (None) values. You will come across this frequently "
    "and we want to show you how to convert them into False values. You will need to use "
    "the is_null() and/or is_not_null() methods."
)
#
#
#
#
#
#
#
#
#
#
#
#

questions[0] = Question(
    """
    Create an event frame by filtering clinical_events to find just the records indicating a diabetes
    diagnosis. (Use the diabetes_codes codelist.)
    """
)
questions[0].expected = clinical_events.where(
    clinical_events.snomedct_code.is_in(diabetes_codes)
)
questions[0]._hint = (
    "Question 0 already has an answer in the quiz.py file. If you want the hint for another "
    "question, make sure to change the '0' in quesions[0].hint(), so you get the hint for the "
    "right question."
)

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

questions[1] = Question(
    """
    Create a patient series containing the date of each patient's earliest diabetes diagnosis.
    """
)

earliest_diagnosis_date = (
    clinical_events.where(clinical_events.snomedct_code.is_in(diabetes_codes))
    .sort_by(clinical_events.date)
    .first_for_patient()
    .date
)
questions[1].expected = earliest_diagnosis_date
questions[1]._hint = (
    "You need to filter clinical_events to just those containing a "
    "diabetes_code, sort the events by date and use first_for_patient "
    "to get the first for each patient.\n\n"
    "Functions like 'where' and 'first_for_patient' return events. An event contains "
    "several properties such as the snomedct code, the date, and optionally a numeric "
    "value. For this question, we only want the date, not the whole event, so you'll "
    "need to append .date to your code."
) + remember_to_sort

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

questions[2] = Question(
    """
    Create a patient series containing the date of each patient's earliest structured education
    programme referral. (Use the referral_code codelist.)
    """
)

earliest_referral_date = (
    clinical_events.where(clinical_events.snomedct_code.is_in(referral_codes))
    .sort_by(clinical_events.date)
    .first_for_patient()
).date

questions[2].expected = earliest_referral_date
questions[2]._hint = (
    "If you've solved question 1 then this is very similar but using the "
    "referral codelist instead of the diabetes codelist. Look back to the "
    "top of the quiz.py file and see which codelists we have provided."
) + remember_to_sort

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

questions[3] = Question(
    """
    Create a boolean patient series indicating whether the date of each patient's earliest diabetes
    diagnosis was between 1st April 2023 and 31st March 2024. If the patient does not have a
    diagnosis, the value for in this series should be False.
    """
)

questions[3].expected = (
    earliest_diagnosis_date.is_not_null()
    & earliest_diagnosis_date.is_on_or_between("2023-04-01", "2024-03-31")
)
questions[3]._hint = (
    "This builds on question 1. Assign your answer to question 1 to a variable like this:\n\n"
    "earliest_diabetes_diagnosis = ...your answer to q1...\n\n"
    "You can then use this new variable in conjunction with one of the date functions e.g. "
    ".is_before(xxx), .is_on_or_after(xxx) etc."
) + boolean_series_none_to_false

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

questions[4] = Question(
    """
    Create a patient series indicating the number of months between a patient's earliest diagnosis
    and their earliest referral.
    """
)
questions[4].expected = (earliest_referral_date - earliest_diagnosis_date).months
questions[4]._hint = (
    "It might be helpful to assign the answers to q1 and q2 into two variables called "
    "earliest_diabetes_diagnosis and earliest_referral. You can then find the difference "
    "between these two dates. Subtracting dates returns a 'DateDifference', so you will also "
    "need to specify how you want that represented e.g. in this case '.months'"
)

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

questions[5] = Question(
    """
    Create a boolean patient series identifying patients who have been diagnosed with diabetes for
    the first time in the year between 1st April 2023 and 31st March 2024, and who have a record of
    being referred to a structured education programme within nine months after their diagnosis.
    """
)

questions[5].expected = (
    earliest_diagnosis_date.is_not_null()
    & earliest_diagnosis_date.is_on_or_between("2023-04-01", "2024-03-31")
    & earliest_referral_date.is_not_null()
    & earliest_referral_date.is_on_or_between(
        earliest_diagnosis_date,
        earliest_diagnosis_date + months(9),
    )
)
questions[5]._hint = (
    "This builds on q3 and q4. You want everyone from q3 who also has their answer to q4 < 9. Note "
    "that data is messy and so there may be people whose referral was before their diagnosis. These "
    "people would have a negative number of months between diagnosis and referral."
) + remember_brackets

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

questions[6] = Question(
    """
    Create a patient series with the date of the latest record of mild frailty for each patient.
    """
)

latest_mild_frailty_date = (
    clinical_events.where(clinical_events.snomedct_code.is_in(mild_frailty_codes))
    .sort_by(clinical_events.date)
    .last_for_patient()
    .date
)
questions[6].expected = latest_mild_frailty_date
questions[6]._hint = (
    "This is the same idea as questions 1 and 2. The only difference is that you want the latest "
    "for each patient rather than the earliest."
) + remember_to_sort

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

questions[7] = Question(
    """
    Create a patient series with the date of the latest record of moderate or severe frailty for
    each patient.
    """
)

latest_moderate_or_severe_frailty_date = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(moderate_frailty_codes)
        | clinical_events.snomedct_code.is_in(severe_frailty_codes)
    )
    .sort_by(clinical_events.date)
    .last_for_patient()
    .date
)
questions[7].expected = latest_moderate_or_severe_frailty_date
questions[7]._hint = (
    (
        "You'll need to either:\n\n"
        " - combine the moderate and severe frailty codelists, which can be done like this: "
        "moderate_frailty_codes + severe_frailty_codes\n"
        " - or make use of the 'or' operator '|' to find snomedct_codes in "
        "moderate_frailty_codes OR in severe_frailty_codes."
    )
    + remember_brackets
    + remember_to_sort
)

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

questions[8] = Question(
    """
    Create a boolean patient series indicating whether a patient's last record of severity is
    moderate or severe. If the patient does not have a record of frailty, the value in this series
    should be False.
    """
)

has_moderate_or_severe_frailty = (
    latest_moderate_or_severe_frailty_date.is_not_null()
    & (
        latest_mild_frailty_date.is_null()
        | (latest_moderate_or_severe_frailty_date.is_after(latest_mild_frailty_date))
    )
)
questions[8].expected = has_moderate_or_severe_frailty
questions[8]._hint = (
    (
        "This is a bit complicated, but essentially we're trying to match one of the following:\n"
        "1. A patient with a moderate or severe frailty code AND no mild frailty code, or\n"
        "2. A patient with a moderate or severe frailty code, AND a mild frailty code, but where "
        "the most recent frailty code is the moderate/severe one.\n"
        "You'll also need the is_null() and is_not_null() methods."
    )
    + remember_brackets
    + boolean_series_none_to_false
)

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

questions[9] = Question(
    """
    Create a patient series containing the latest HbA1c measurement for each patient.
    """
)

latest_hba1c_measurement = (
    clinical_events.where(clinical_events.snomedct_code.is_in(hba1c_codes))
    .sort_by(clinical_events.date)
    .last_for_patient()
    .numeric_value
)

questions[9].expected = latest_hba1c_measurement
questions[9]._hint = (
    "Remember to:\n\n"
    "- use the hba1c_codes codelist\n"
    "- use the last_for_patient() method - but only after sorting the events\n"
    "- use the `numeric_value` property to get the actual value"
) + remember_to_sort

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

questions[10] = Question(
    """
    Create a boolean patient series identifying patients without moderate or severe frailty in whom
    the last IFCC-HbA1c is 58 mmol/mol or less
    """
)

questions[10].expected = (
    has_moderate_or_severe_frailty.is_null()
    & latest_hba1c_measurement.is_not_null()
    & (latest_hba1c_measurement <= 58)
)
questions[10]._hint = (
    "This makes use of the answers to 8 and 9. It makes things tidier if you:\n"
    "- assign the answer to q8 to a variable called most_recent_frailty_is_moderate_or_severe\n"
    "- assign the answer to q9 to a variable called latest_hba1c_value\n"
    "You then need ehrql to represent people who DON'T have moderate/severe frailty AND whose "
    "most recent hba1c was <= 58."
) + remember_brackets
