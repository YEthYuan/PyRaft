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
        return self.get_entry_term(self.last_log_index)

    def log_append(self, new_entries):
        self.entries.extend(new_entries)

    def log_delete(self, index):
        if index < 0 or index >= len(self.entries):
            return
        self.entries = self.entries[: max(0, index)]

    def get_entry_term(self, index):

        if index < 0 or index >= len(self.entries):
            return -1
        else:
            return self.entries[index]['term']

    def get_entry(self, index):
        return self.entries[max(0, index)]

    def get_entries(self, index):
        return self.entries[max(0,index):]

    def print_log(self):
        for entry in self.entries:
            print(entry)

    def save(self):
        with open(self.filename, "w") as f:
            json.dump(self.entries, f, indent=4)

def run():
    filename = str(1)+'_log.json'
    log = Log(filename)

    print(log.get_entries(0))
    info1 = {
        'dict_id':1,
        'client_ids':[1,2,3],
        'public key':11111
    }

    info2 = {
        'dict_id':1,
        'client_id':1,
        'key': 'wxw',
        'value':10
    }
    log.log_append([info1,info2])

    log.print_log()
    log.save()



if __name__ == '__main__':
    run()




