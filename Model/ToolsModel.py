import re
class Tools:
    @staticmethod
    def remove_caratcrer(str_extract):
        return re.sub('[^0-9]', '', str_extract)
