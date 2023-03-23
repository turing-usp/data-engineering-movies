class DeserializeMixin:
    def fromDict(self, init_dict):
        for key, value in init_dict.items():
            setattr(self, key, value)