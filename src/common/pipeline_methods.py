import math

def simple_grep(text_line: str, search_term: str):
    """
    Loosely searches a line of text for a specified search term.

    This is based on Google's Apache Beam example which only checks for the term at start of a line.

    :param str text_line: Line of text to search
    :param str search_term: Term to find in the text
    :return str text_line: Lines of text starting with the search term
    """

    # Using a generator
    # https://beam.apache.org/documentation/transforms/python/elementwise/flatmap/#example-4-flatmap-with-a-generator
    if text_line.startswith(search_term):
        yield text_line


def java_package_name_split(package_name: str) -> list[str]:
    """
    Splits a fully qualified Java package name into its parts, e.g.:
        com.example.appname.library.widgetname becomes:
        [
            "com",
            "com.example",
            "com.example.appname",
            "com.example.appname.library",
            "com.example.appname.library.widgetname"
        ]

    This is based on Google's Apache Beam examples.

    :param str package_name: Fully qualified Java package name
    :return list[str] result: List of Java packages split up as per the above example
    """
    PACKAGE_SEP = "."

    # Google's original example does a classic loop and shifting along the package separator indices gradually
    # This can be done with classic list comprehensions
    separator_indices = [
        index for index, char in enumerate(package_name) if char == PACKAGE_SEP
    ]
    result = [package_name[0:next_index] for next_index in separator_indices]

    # Don't forget the package name itself
    result.append(package_name)

    return result


def get_packages(text_line: str, search_term: str) -> list[str]:
    JAVA_LINE_END = ";"
    result = []

    start_index = text_line.find(search_term) + len(search_term)
    end_index = text_line.find(JAVA_LINE_END, start_index)

    if start_index < end_index:
        package_name = text_line[start_index:end_index].strip()
        result = java_package_name_split(package_name=package_name)

    return result


def count_package_use(text_line: str, search_term: str) -> tuple[str, int]:
    packages = get_packages(text_line=text_line, search_term=search_term)

    for next_pkg in packages:
        yield (next_pkg, 1)

def package_help(record: str, keyword: str):
    package_name: str = ''

    if (record is not None):
        lines = record.split('\n')
        todos = len([line for line in lines if 'FIXME' in line or 'TODO' in line])

        for line in lines:
            if (line.startswith(keyword)):
                package_name = line

        packages = get_packages(text_line=package_name, search_term=keyword)

        for pkg in packages:
            yield (pkg, todos)

def composite_score(popular, help: dict):
    for element in popular:
        if help.get(element[0]):
            composite = math.log(help.get(element[0])) * math.log(element[1])
            if (composite > 0):
                yield (element[0], composite)