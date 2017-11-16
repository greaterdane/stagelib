import os, sys
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
from fileIO import Folder, File

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
        self.parser.add_argument('-a', dest = 'a', help = 'This is an argument for the main program with switch "a".')
        self.parser.add_argument('-b', dest = 'b', help = 'This is an argument for the main program with switch "b"')

class Command(object):
    def __init__(self, args):
        self.args = args

    @staticmethod
    def add_switches(sub_parser): # add arguments here.
        sub_parser.add_argument('-a', dest='command1_a', help='This is an argument for "command1" with switch "a".')
        sub_parser.add_argument('-b', dest='command1_b', help='This is an argument for "command1" with switch "b".')

    def validate(self):
        pass

    def set_context_from_args(self):
        self.validate()

    def execute(self):
        self.set_context_from_args()

class Processor(Program):
    def __init__(self, **kwds):
        super(Processor, self).__init__(description = "Command line tool for data preparation.", **kwds)

    def _add_switches(self): # add arguments here.
        self.parser.add_argument('schema', help = "Table structure or schema you wish to conform to.")
        self.parser.add_argument('--dirname', help = "Directory containing input files.  Defaults to your current working directory.", default = os.getcwd())
        self.parser.add_argument('-i', nargs = '?', help = "One or more files for input.", dest = 'infile')
        self.parser.add_argument('-o', help = "Output file.", dest = 'outfile')
        self.parser.add_argument('-m', '--pattern', help = "Pattern to match when filtering a directory, e.g. '*.csv' or 'file.*?.json' (regexes are allowed).", dest = 'pattern')
        self.parser.add_argument('-r', action = 'store_true', default = False, help = 'Flag to search directory recursively', dest = 'recurisive')        
        self.parser.add_argument('-d', action = 'store_true', default = False, help = 'Flag to process distinct files only.', dest = 'distinct')
        self.parser.add_argument('--unzip', action = 'store_true', default = False, help = 'Flag to unzip archives in directory.')

class DBCommand(Command):
    @staticmethod
    def add_switches(sub_parser): # add arguments here.
        sub_parser.add_argument('-t', dest = 'table')
        sub_parser.add_argument('-u', dest = 'user')
        sub_parser.add_argument('-p', dest = 'passwd')
        sub_parser.add_argument('-db', help = "If database not specified in table name, i.e. db.`table`, add it here.")
        sub_parser.add_argument('-host')
        sub_parser.add_argument('-port', default = 3306, type = int)

class Listdir(Command):
    pass
    
class Normalize(Command):
    pass

## Add command maps here.
COMMAND_MAP = {
    'listdir': {'class': Listdir, 'desc': 'Show the contents of a given directory.'},
    'normalize': {'class': Normalize, 'desc': 'Prepare dataset or file database import and analysis.'},
        }

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
    launch(Processor, COMMAND_MAP)
