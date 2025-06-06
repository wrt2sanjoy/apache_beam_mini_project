from datetime import datetime
import heapq
import apache_beam as beam

# --- The functions remain the same ---

def normalize_date(date_string):
    """
    Parses a date string from known formats and returns it as 'YYYY-MM-DD'.
    """
    known_formats = ['%d-%m-%Y', '%d/%m/%Y']
    for fmt in known_formats:
        try:
            date_obj = datetime.strptime(date_string.strip(), fmt)
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None

def get_top_departments(element):
    """
    Finds the top 3 departments from a list of (department, count) tuples.
    """
    date, departments = element
    top3 = heapq.nlargest(3, departments, key=lambda dept: dept[1])
    return date, top3

# --- Create and run the pipeline ---

with beam.Pipeline() as p1:
    result = (
        p1
        | "Read input" >> beam.io.ReadFromText(r"data\hospital_visit_data.csv", skip_header_lines=1)
        | "Split each line on ','" >> beam.Map(lambda record: record.split(','))
        | "Normalize the date field" >> beam.Map(lambda record: (normalize_date(record[4]), record[3]))
        | "Filter Bad dates" >> beam.Filter(lambda record: record[0] is not None)
        | "Create daily dept key" >> beam.Map(lambda record: (record, 1))
        | "Count visit" >> beam.CombinePerKey(sum)
        | "Set date as key" >> beam.Map(lambda element: (element[0][0], (element[0][1], element[1])))
        | "Group by date" >> beam.GroupByKey()
        | "Find top 3 per day" >> beam.Map(get_top_departments)
        | "Filter for Days with 3 Results" >> beam.Filter(lambda element: len(element[1]) >= 3)
        | "Save to a text file" >> beam.io.WriteToText(r"data\results_top_3_only")
    )
