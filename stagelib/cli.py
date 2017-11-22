import os, sys
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
from generic import attrdict, mergedicts, filterdict
from files import Folder, File
import dataframe

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

    def set_context_from_args(self, filters = [], **kwds):
        self.validate()
        if hasattr(self.args, 'sub_command'):
            delattr(self.args, 'sub_command')
        self.args.kwds = filterdict(attrdict(self.args), filters, **kwds)

    def execute(self):
        self.set_context_from_args()

class Processor(Program):
    def __init__(self, **kwds):
        super(Processor, self).__init__(description = "Command line tool for data preparation.", **kwds)

    def _add_switches(self): # add arguments here.
        self.parser.add_argument('-o', default = '', help = "Output file for task result.", dest = 'outfile')
        self.parser.add_argument('-d', nargs = '?', help = "Directory containing input files.  Defaults to your current working directory.", default = '.', dest = 'dirname')
        self.parser.add_argument('-m', '--pattern', default = '', help = "Pattern to match when filtering a directory, e.g. '*.csv' or 'file.*?.json' (regexes are allowed).", dest = 'pattern')
        self.parser.add_argument('-r', action = 'store_true', default = False, help = 'Flag to search directory recursively', dest = 'recurisive')        
        self.parser.add_argument('-D', action = 'store_true', default = False, help = 'Flag to list distinct files only.', dest = 'distinct')
        self.parser.add_argument('-F', action = 'store_true', default = False, help = 'Flag to list files only.', dest = 'files_only')
        self.parser.add_argument('--unzip', action = 'store_true', default = False, help = 'Flag to unzip archives in directory.')
        self.parser.add_argument('--showlist', action = 'store_true', default = True, help = 'Flag indicating only to print contents of a given directory.')

class ProcessorCommand(Command):
    def set_context_from_args(self):
        filters = ['pattern', 'unzip', 'distinct', 'files_only', 'recurisive']
        super(ProcessorCommand, self).set_context_from_args(filters = filters)
        self.args.listkwds = self.args.kwds
        filters.extend(['showlist', 'dirname', 'infile', 'outfile', 'kwds', 'listkwds'])
        super(ProcessorCommand, self).set_context_from_args(filters = filters, inverse = True)
    
class DBCommand(Command):
    @staticmethod
    def add_switches(sub_parser): # add arguments here.
        sub_parser.add_argument('-t', dest = 'table')
        sub_parser.add_argument('-u', dest = 'user')
        sub_parser.add_argument('-p', dest = 'passwd')
        sub_parser.add_argument('-db', help = "If database not specified in table name, i.e. db.`table`, add it here.")
        sub_parser.add_argument('-host')
        sub_parser.add_argument('-port', default = 3306, type = int)

class Listdir(ProcessorCommand):

    @staticmethod
    def func(dirname, **kwds):
        return Folder.table(dirname, **kwds)
    
    @staticmethod
    def showlist(dirname, **kwds):
        print Listdir.func(dirname, **kwds).prettify()

    def execute(self):
        super(Listdir, self).execute()
        _ = self.args.dirname
        __ = self.args.listkwds
        if self.args.showlist:
            print; print "Displaying contents of '{}'\n".format(_ if _ != '.' else os.getcwd())
            self.showlist(_, **__)
        return self.func(_, **__)

class Normalize(ProcessorCommand):
    @staticmethod
    def func(row):
        raise NotImplementedError
    
    @staticmethod
    def listdir(dirname, commandmap = {}, **kwds):
        if not commandmap:
            commandmap = COMMAND_MAP
        return commandmap['listdir']['class'].func(dirname, **kwds)

    @staticmethod
    def add_switches(sub_parser):
        sub_parser.add_argument('schema_name', nargs = '?', help = 'Name specifying table structure and nature of data to normalize.')
        sub_parser.add_argument('-i', default = '', nargs = '?', help = "One or more files for input.", dest = 'infile')
        
    def set_context_from_args(self):
        super(Normalize, self).set_context_from_args()
        self.list = self.listdir(self.args.dirname, **self.args.listkwds)

    def execute(self):
        super(Normalize, self).execute()
        for row in self.list.itertuples():
            self.func(row)

## Add command maps here.
COMMAND_MAP = {
    'listdir': {'class': Listdir, 'desc': 'Show the contents of a given directory.'},
    'normalize': {'class': Normalize, 'desc': 'Prepare dataset or file database import and analysis.'},
        }

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
    launch(Processor, COMMAND_MAP)
