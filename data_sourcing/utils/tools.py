from dateutil.parser import parse

class Tools():
    def getYearValue(Id):
        try:
            return parse(Id, fuzzy=True).year
        except:
            return 0