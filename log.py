import os
import json


class Log:
    def __init__(self, filename):
        self.filename = filename

        if os.path.exists(self.filename):
            with open(self.filename, "r") as f:
                self.entries = json.load(f)
        else:
            self.entries = []

    @property
    def last_log_index(self):
        return len(self.entries)-1

    @property
    def last_log_term(self):
        return self.entries[self.last_log_index]['term']

    def log_append(self, info: dict, act: str, term: int):

        """
        Add new entry to log

        :param info: contain all the information
        :param act: three types of entry(create,put,get), use to recover
        :param term: current term

        :return: None

        """
        entry = {
            'term': term,
            'info': info,
            'act': act
        }
        self.entries.append(entry)

    def print_log(self):
        for entry in self.entries:
            print(entry)

    def save(self):
        with open(self.filename, "w") as f:
            json.dump(self.entries, f, indent=4)

def run():
    filename = str(1)+'_log.json'
    log = Log(filename)

    info = {
        'dict_id':1,
        'client_ids':[1,2,3],
        'public key':11111
    }
    log.log_append(info,'create',1)

    info = {
        'dict_id':1,
        'client_id':1,
        'key': 'wxw',
        'value':10
    }
    log.log_append(info,'put',2)

    log.print_log()
    log.save()



if __name__ == '__main__':
    run()




