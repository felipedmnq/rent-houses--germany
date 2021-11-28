# connect to postgresql database

from configparser import ConfigParser
 
def config(filename='database.ini', section='postgresql'):
    '''Connects with local postgresql database
    
    Parameters:
    -----------
        filename: str
            file to extract the connection infos.
        section: str
    
    Return:
    -------
        Connections infos to be used in the database connection.
    '''
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
 
    # get section, default to postgresql
    db = {}
    
    # Checks to see if section (postgresql) parser exists
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
         
    # Returns an error if a parameter is called that is not listed in the initialization file
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
 
    return db