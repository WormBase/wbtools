from wbtools.literature.ack_form.caltech import CaltechData


def make_data(**overrides) -> CaltechData:
    """a CaltechData with every field empty, overridden by keyword"""
    data = CaltechData()
    for field, value in overrides.items():
        if not hasattr(data, field):
            raise AttributeError(field)
        setattr(data, field, value)
    return data
