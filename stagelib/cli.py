import os, sys
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
from generic import attrdict, mergedicts, filterdict
from files import Folder, File
import dataframe

DISPLAY = """
CONTENTS OF '{}'""".format

def getborder(x, char = '*'):
    return char * len(x)

def launch(program, commandmap): #launch the program
    program.start(commandmap)

class Program(object):
    def __init__(self, description = '', **kwds):
        self.parser = ArgumentParser(description = description, formatter_class = RawTextHelpFormatter, **kwds)
        self._add_switches()
        self.subparsers = self.parser.add_subparsers(dest='sub_command')

    @classmethod
    def start(cls, sub_command_map):
        program = cls() # construct the main program
        program.add_subparsers(sub_command_map) #add commands
        args = program.parse_args()
        sub_command = sub_command_map[args.sub_command]['class'](args) #args.sub_command contains the name of the command
        sub_command.execute()

    def get_groups():
        for sub_command in sub_command_map:
            sub_command_details = sub_command_map[sub_command]
            new_command_parser = self.subparsers.add_parser(sub_command, help=sub_command_details['desc'])
            if hasattr(sub_command_details['class'], 'add_switches'):
                sub_command_details['class'].add_switches(new_command_parser)

    def add_subparsers(self, sub_command_map): # here is where commands are evaluated.
        for sub_command in sub_command_map:
            sub_command_details = sub_command_map[sub_command]
            new_command_parser = self.subparsers.add_parser(sub_command, help=sub_command_details['desc'])
            if hasattr(sub_command_details['class'], 'add_switches'):
                sub_command_details['class'].add_switches(new_command_parser)

    def parse_args(self, *args):
        return self.parser.parse_args(*args)

    def _add_switches(self): # add arguments here.
        pass

class Command(object):
    def __init__(self, args):
        self.args = args

    @staticmethod
    def add_switches(sub_parser): # add arguments here.
        pass

    def validate(self):
        pass

    def set_context_from_args(self, filters = []):
        kwds = {}
        for name in filters:
            kwds[name] = getattr(self.args, name)
            delattr(self.args, name)
        self.args.kwds = kwds
        self.validate()

    def execute(self):
        self.set_context_from_args()

class Stagelib(Program):
    def __init__(self, **kwds):
        super(Stagelib, self).__init__(description = "Command line tool for data preparation.", **kwds)

    def _add_switches(self): # add arguments here.
        self.parser.add_argument('-o', default = '', help = "Output file for task result.", dest = 'outfile')
        self.parser.add_argument('-i', nargs = '?', help = "One or more files for input.", dest = 'infile')
        self.parser.add_argument('-d', nargs = '?', help = "Directory containing input files.  Defaults to your current working directory.", default = os.getcwd(), dest = 'dirname')
        self.parser.add_argument('-m', '--pattern', default = '', help = "Pattern to match when filtering a directory, e.g. '*.csv' or 'file.*?.json' (regexes are allowed).", dest = 'pattern')
        self.parser.add_argument('-r', action = 'store_true', default = False, help = 'Flag to search directory recursively', dest = 'recurisive')        
        self.parser.add_argument('-D', action = 'store_true', default = False, help = 'Flag to list distinct files only.', dest = 'distinct')
        self.parser.add_argument('-F', action = 'store_true', default = False, help = 'Flag to list files only.', dest = 'files_only')
        self.parser.add_argument('--unzip', action = 'store_true', default = False, help = 'Flag to unzip archives in directory.')
        self.parser.add_argument('--showlist', action = 'store_true', default = True, help = 'Flag indicating only to print contents of a given directory.')

class StageCommand(Command):
    def set_context_from_args(self):
        super(StageCommand, self).set_context_from_args(
            filters = ['pattern',
                       'unzip',
                       'distinct',
                       'files_only',
                       'recurisive'])

class Listdir(StageCommand):
    @staticmethod
    def func(args):
        return Folder.table(args.dirname, **args.kwds)

    @staticmethod
    def show(dirlist, **kwds):
        print dirlist.prettify()

    def __call__(self, **kwds):
        dl = self.func(self.args)
        if self.args.showlist:
            print DISPLAY(self.args.dirname)
            self.show(dl, **kwds)
        return dl

    def execute(self):
        super(Listdir, self).execute(); self()

class Normalize(StageCommand):
    @staticmethod
    def func(args):
        pass

    @staticmethod
    def add_switches(sub_parser):
        sub_parser.add_argument('-s', '--schema', help = 'Name specifying table structure and nature of data to normalize.')
        sub_parser.add_argument('-t', dest = 'table', nargs = '?', help = 'If normalization is successful, import files info specified database table.')
        sub_parser.add_argument('--load_db', action = 'store_true', default = False, help = 'Flag indicating that normalized csvfiles can be imported into database.')
        sub_parser.set_defaults(showlist = False)

class DatabaseCommand(Command):
    @staticmethod
    def add_switches(sub_parser): # add arguments here.
        sub_parser.add_argument('-t', dest = 'table')
        sub_parser.add_argument('-u', dest = 'user')
        sub_parser.add_argument('-p', dest = 'passwd')
        sub_parser.add_argument('-db', help = "If database not specified in table name, i.e. db.`table`, add it here.")
        sub_parser.add_argument('-host')
        sub_parser.add_argument('-port', default = 3306, type = int)

    def set_context_from_args(self):
        super(DatabaseCommand, self).set_context_from_args(
            filters = ['table',
                       'user',
                       'passwd',
                       'port',
                       'host'])

class LoadDatabase(DatabaseCommand):
    @staticmethod
    def func(args):
        pass    

COMMAND_MAP = {
    'listdir': {'class': Listdir, 'desc': 'Show the contents of a given directory.'},
    'normalize': {'class': Normalize, 'desc': 'Prepare dataset or file database import and analysis.'},
    'load_db' : {'class' : LoadDatabase, 'desc' : 'Import csvfiles into mysql database.'}
        }

if __name__ == '__main__':
    launch(Stagelib, COMMAND_MAP)
