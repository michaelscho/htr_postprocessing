import json # for handling JSON data
import pandas as pd # for handling data
import re # for handling regex
import requests # for handling http request to latin text archive
import config # for basic configuration
from csv import DictWriter # for writing CSV files from dictionaries


class TextForPostprocessing:
    """Creates instance of text object that gets processed.

    Creates instance of text object that gets processed and provides functions for processing.
    
    :param path_to_textfile: path to textfile as string. 
    :param path_to_textfile: characters that need to be deleted from text such as interpunctuation. Should be specified in config.py.
    """

    def __init__(self, path_to_textfile, characters_to_clean):
        """ Initializes and preprocesses object

        The textobject gets initialized from path. Then, some special characters are deleted and a list of unique words is created.   
        """
        # Initialize variables ...
        self.path_to_textfile = path_to_textfile
        # ... read in text from textfile ...
        self.text = self.read_text()
        # ... clean up text using list of characters to be deleted ...
        self.clean_text(characters_to_clean)    
        # ... make list of unique words from text
        self.unique_words = self.create_unique_wordlist()

    def read_text(self):
        """ Read text from file

        Helper function for reading the text file.

        :return text: returns text as string.
        """
        # Read in plain text file as input
        with open(self.path_to_textfile, 'r') as f:
            text = f.read()
        return text

    def write_text(self, filepath):
        """ Write text to file

        Helper function for writing the text to a file.

        :param filepath: External filepath as string.
        """

        with open(filepath, 'w+') as f:
            f.write(self.text)

    def create_unique_wordlist(self):
        """ Creation of unique wordlist

        Creates a unique wordlist from all words used in the text to iterate through it in the expansion process.

        :return unique_words: returns list of unique words.
        """
        # Split text into list of words ...
        wordlist_from_text = self.text.split()
        # Keep only unique words using set
        unique_words = list(set(wordlist_from_text))
        return unique_words

    def clean_text(self, characters_to_clean):
        """ Cleaning of text

        Cleans text from given special characters.

        :param characters_to_clean: Provided through __init__
        """
        # Iterate through list of special characters...
        for character in characters_to_clean:
            #... delete character 
            self.text = self.text.replace(character, '')

    def replace_word_in_text(self, word):
        """ Replacement of words

        Takes word from Expansion object and replaces in text. 
        """

        # Replaces word in text. Whitespace and Linebreak added to prevent deletion of substrings. 
        self.text = re.sub(r' ' + word[0] + r' ', r' ' + word[1] + " ", self.text)
        self.text = re.sub(r'\n' + word[0] + r' ', r'\n' + word[1] + " ", self.text)
        self.text = re.sub(r' ' + word[0] + r'\n', r' ' + word[1] + "\n", self.text)


class Expansion:
    """Creates instance of Expansion object.

    Creates instance of Expansion object that expands words given to object.
    
    :param special_characters: Dictionary of special characters provided by config.py for identification of abbreviations. 
    :param abbreviation_dictionary: Dictionary containing abbreviations and expansions created from class Dictionary() for automatic expansion via wordlist.
    :param rules_for_expansion: Rules for manual expansion abbreviation is not in abbreviation_dictionary.
    """

    def __init__(self, special_characters, abbreviation_dictionary, rules_for_expansion):
        self.special_characters = special_characters
        self.abbreviation_dictionary = abbreviation_dictionary
        self.rules_for_expansion = rules_for_expansion
        # List of expanded words for later replacement. 
        self.expanded_words = []
        # Error-List for debugging.
        self.expanded_words_errors = []

    def expand_abbreviation(self, word):
        """ Expansion of abbreviated word

        Expands abbreviated word provided by TextForPostprocessing.unique_words and stores word in self.expanded_words.

        :param word: Abbreviated word as string.
        """

        # Check if word is abbreviated using predefined special characters ...
        if any(character in self.special_characters for character in word):
            # ... try expansion by wordlist ...
            if word in self.abbreviation_dictionary:
                #... and get corresponding expansion if found in dictionary.
                    expansion = self.abbreviation_dictionary[word]
            # Otherwise perform expansion by rules ...
            else:
                expansion = word
                # ... iterate through rules for expansion and replace special characters
                for key, value in self.rules_for_expansion.items():
                    expansion = expansion.replace(key,value)
            # create list of abbreviation and corresponding expansion and add to expanded_words list.
            self.expanded_words.append([word, expansion])
        # if no special character in word, proceed to next word in loop, because word cannot be an abbreviation.
        else:
            return


class Lexicon:
    """Creates instance of Lexicon object.

    Creates instance of Lexicon object and provides functions for reading of content either by http query or local list. 
    Lexicon should be Frankfurt Latin Lexicon (https://lta.bbaw.de/lexicon/). Otherwise, functions need to be changed accordingly.
    
    :param path_to_lexicon: Path to local copy of Frankfurt Latin Lexicon. 
    :param url_to_lexicon: Query URL for http request as string (Should be 'https://lta.bbaw.de//lexicon/fll/process/wf?columns[2][search][value]='. Searchstring should be added after '=').
    """

    def __init__(self, path_to_lexicon, url_to_lexicon):
        self.path_to_lexicon = path_to_lexicon
        self.url_to_lexicon = url_to_lexicon
        self.df = self.load_data()
        self.df_online = {}

    def load_data(self):
        """ Load local copy of Frankfurt Latin Lexicon

        Load local copy of Frankfurt Latin Lexicon as pandas dataframe for further processing in chunks to cope with large file.
        File uses tabs as separator.

        :return df: Lexicon as pandas dataframe.
        """

        # Load lexicon as pandas dataframe in chunks due to its size
        chunk = pd.read_csv(self.path_to_lexicon, sep='\t', on_bad_lines='skip', chunksize=100000)
        df = pd.concat(chunk)
        return df

    def query_frankfurt_text_lexicon(wordlist):
        """ Query Frankfurt Latin Lexicon online

        Query wordlist in Frankfurt Latin Lexicon online and save as JSON file for further usage.
        Example query: query_to_lexicon = requests.get('https://lta.bbaw.de//lexicon/fll/process/wf?columns[2][search][value]=dampnabitur')
        Example response: 
        '{"draw":null,
            "recordsTotal":9642318,
            "data":[
                    ["<a href=\\"https://lta.bbaw.de/lexicon/fll/wf?sl=4683867\\" title=\\"4684631\\">damno@V</a>",
                    "<a href=\\"https://lta.bbaw.de/lexicon/fll/wf?l=4684305\\">dampno</a>",
                    "dampnabitur","V","","","SINGULAR","PERSON_3","FUTURE","INDICATIVE","PASSIVE"]
                   ],
            "recordsFiltered":1}'

        :param wordlist: Words to query as list.
        :return list_of_words_in_lexicon: List of words in lexicon as list.
        """

        list_of_words_in_lexicon = []
        # Iterates through wordlist ...
        for word in wordlist:
            # ... queries word in FLL ...
            query_to_lexicon = requests.get(self.url_to_lexicon + word)
            # ... takes response as JSON ...
            response_dict = json.loads(query_to_lexicon)
            # ... takes data field of response ...
            entry_list = response_dict['data']
            # ... takes first entry in data list
            entry = entry_list[0]
            # ... appends to list
            list_of_words_in_lexicon.append(entry)
        with open('../output/data_frankfurt_lexicon.json', 'w+') as f:
            f.write(list_of_words_in_lexicon)
        return list_of_words_in_lexicon

    def open_existing_word_list():
        """ Load existing JSON file

        """

        with open('../output/data_frankfurt_lexicon.json', 'r') as f:
            list_of_words_in_lexicon = f.read()
            return list_of_words_in_lexicon

    def proof_read_word(self, word):
        """ Proofreading word

        Proofreades given word using Lexicon. Returns True or False if word is in Lexicon.

        :param word: Word to proofread as string.
        :return: Boolean. 
        """

        if self.df['WF-Name'].eq(word[1].lower()).any():
            return True
        else:
            return False


class Dictionary:
    """Creates instance of Dictionary object.

    Creates Dictionary object containing abbreviations from JSON file and returns dictionary.

    :param path_to_dictionary: Path to JSON file as string.
    """

    def __init__(self, path_to_dictionary):
        """Initialiszation of Dictionary object.

        """

        self.path_to_dictionary = path_to_dictionary
        self.dictionary = self.load_abbreviation_dict()

    def load_abbreviation_dict(self):
        """ Loads dictionary from JSON file

        :return dictionary: Abbreviations and corresponding expansions as dictionary.
        """

        # Opens JSON file...
        with open(self.path_to_dictionary,'r') as json_file:
            # ... and loads as JSON
            dictionary = json.load(json_file)
        return dictionary

class Normalisation:
    """Creates instance of Normalisation object.

    Creates Normalisation object for normalising words using Frankfurt Latin Lexicon.
    Takes pandas dataframe provided by Lexicon object.
    """

    def __init__(self, lexicon_df):
        """Initialiszation of Dictionary object.

        :param lexicon_df: pandas dataframe provided by Lexicon object.
        """

        self.df = lexicon_df
        self.normalisation_error_list = []

    def word_segmentation(self, text):
        """ Deletes linebreaks within words

        Cearches word before and after linebreak, puts them together to an evaluation term
        and checks if evaluation term exists in Frankfurt Latin Lexicon. If new word exists, linebreak is deleted.

        :param text: text provided by TextForPostprocessing.text
        :return text: gives back text with proper linebreaks between words as string
        """

        # Compiles pattern of 'word linebreak word'...
        pattern = re.compile(r'\b\w+\n\w+\b', re.MULTILINE)
        # ... finds all instances of pattern...
        linebreaks = re.findall(pattern, text)
        # iterates through instances ...
        for instance in linebreaks:
            # creates evaluation term by deleting linebreak... 
            evaluation_term = instance.replace('\n','')
            
            if self.df['WF-Name'].eq(evaluation_term.lower()).any():
                text = text.replace(instance, evaluation_term)
            
        return text

    def normalise_text(self, sample_text):
        # Split text into list of words ...
        wordlist_from_text = sample_text.split()
        # Keep only unique words using set
        unique_words = list(set(wordlist_from_text))
        # iterate through list and check lexicon
        for word in unique_words:
            word_to_test = word

            if self.df['WF-Name'].eq(word_to_test.lower()).any():
                # Get superlemma of that row
                superlemma = self.df.loc[self.df['WF-Name'] == word_to_test.lower(), 'SL-Name'].array[0]
                # Get lemma of that row
                lemma = self.df.loc[self.df['WF-Name'] == word_to_test.lower(), 'L-Name'].array[0]

                # add some stopwords
                if superlemma == "alea@NN" or superlemma == "a@AP" or superlemma == "hilla@NN":
                    continue
                if superlemma.split("@")[0][-1:] != lemma[-1:]:
                    print("Fehler in Lemma oder Superlemma: " + superlemma, lemma)
                    continue

                # create dictionary with word, lemma and superlemma
                dict_normalization = {"Wort": word, "Superlemma": superlemma, "Lemma": lemma}
                
                # Delete last character to get root of word
                wordform = superlemma.split("@")[1]

                # decide wordform to deduce root of word
                if wordform == 'V':
                    superlemma = superlemma.split("@")[0]
                    if superlemma.endswith('or'):
                        chars_to_delete = -2
                    elif superlemma.endswith('sco'):
                        chars_to_delete = -3
                    else:
                        chars_to_delete = -1
                    superlemma = superlemma[:chars_to_delete] # replace last char to get root
                    lemma = lemma[:chars_to_delete] # replace last char to get root
                elif wordform == 'ADV':
                    superlemma = superlemma.split("@")[0] # take as it is
                    lemma = lemma # take as it is
                elif wordform == 'PRO':
                    if superlemma.endswith('er'):
                        chars_to_delete = -2
                    else:
                        chars_to_delete = -1

                    superlemma = superlemma.split("@")[0][:chars_to_delete] # replace last char to get root
                    lemma = lemma[:chars_to_delete] # replace last char to get root

                else:
                    superlemma = superlemma.split("@")[0]
                    endings = ["um", "us", "u", "e", "os", "a", "us", "is", "es", "as"] 
                    for ending in endings:
                        if superlemma.endswith(ending):
                            superlemma = superlemma[:-len(ending)]
                            lemma = lemma[:-len(ending)]

                # update dictionary with manipulated forms
                dict_normalization.update({"Superlemma_root": superlemma, "Lemma_root": lemma, "Wortform": wordform}) 

                # check if word started with capital
                if word[0].isupper():
                    lemma = lemma.capitalize() 
                    superlemma = superlemma.capitalize() 

                # Normalise word by replacing lemma with superlemma
                normalised_word = word.replace(lemma, superlemma)

                # normalise endings of verbs
                if wordform == 'V':
                    ending = normalised_word.replace(superlemma, '')
                    for word_ending in config.verb_endings_to_normalise:
                        normalised_word = normalised_word.replace(word_ending[0], word_ending[1])
                
                # update dictionary with manipulated forms
                dict_normalization.update({"Normalisierung": normalised_word}) 

                # replace normalised_word in sample text
                #sample_text = sample_text.replace(word, normalised_word)
                sample_text = re.sub(r' ' + word + r' ', r' ' + normalised_word + r' ', sample_text)
                sample_text = re.sub(r'\n' + word + r' ', r'\n' + normalised_word + r' ', sample_text)
                sample_text = re.sub(r' ' + word + r'\n', r' ' + normalised_word + r'\n', sample_text)

            else:
                self.normalisation_error_list.append(word)

            # Normalise some characters that could still be in ending
            characters_to_normalise = config.characters_to_normalise
            for character in characters_to_normalise:
                sample_text = sample_text.replace(character[0], character[1])

            # write dictionary to csv for debugging purpose
            self.write_to_csv(dict_normalization)

        # Normalise some characters that could still be in ending
        characters_to_normalise = config.characters_to_normalise
        for character in characters_to_normalise:
            sample_text = sample_text.replace(character[0], character[1])
            
        return sample_text

    def set_csv_fieldnames(self, fieldnames):
        self.fieldnames = fieldnames

    def write_to_csv(self, dict):
        with open('../output/normalized_words.csv', 'a') as f:
            append_to_csv = DictWriter(f, fieldnames=['Wort', 'Superlemma', 'Lemma', 'Superlemma_root', 'Lemma_root', 'Wortform', 'Normalisierung'])
            append_to_csv.writerow(dict)


def main():

    # Set path to plain text file exported from HTR
    path_to_sample_text = '../input/BAV_Pal_lat_586_13.txt'

    # Define list of utf-character that need to be deleted (specified in config.py)
    characters_to_clean = config.characters_to_clean

    # Define list of utf-character indicating abbreviated words (specified in config.py)
    special_characters_dictionary = config.special_characters_dictionary

    # Convert to list
    special_characters = list(special_characters_dictionary.values())

    # Set rules for manual expansion (specified in config.py)
    rules_for_expansion = config.rules_for_expansion

    # Set path to abbreviation dictionary
    path_to_abbreviation_dictionary = '../ressources/abbreviation_dictionary.json'

    # Inititalise abbreviation dictionary
    abbreviations = Dictionary(path_to_abbreviation_dictionary)

    # Set path to latin dictionary
    path_to_latin_dictionary = '../ressources/frankfurt_latin_lexicon.txt'

    # Set url to lexicon
    url_to_lexicon = 'https://lta.bbaw.de//lexicon/fll/process/wf?columns[2][search][value]='

    # Create sample_text object
    sample_text = TextForPostprocessing(path_to_sample_text, characters_to_clean)
    
    # Create Expansion object
    expansions = Expansion(special_characters, abbreviations.dictionary, rules_for_expansion)

    # Get list of unique words in sample_text and iterate through it...
    for word in sample_text.unique_words:
        # ... expand word if it is abbreviated 
        expansions.expand_abbreviation(word)
       
    # check if expanded word exists in lexicon
    # Inititalise lexicon
    lexicon = Lexicon(path_to_latin_dictionary, url_to_lexicon)

    # Iterate through list of expanded abbreviations and check them
    for word in expansions.expanded_words:
        #true_or_false = lexicon.proof_read_word(word)
        true_or_false = True
        # If word exist in lexicon, replace in sample text
        if true_or_false == True:
            sample_text.replace_word_in_text(word)
            
        # Else append word to list of wrong expansion
        else:
            sample_text.replace_word_in_text(word)
            expansions.expanded_words_errors.append(word)

    # Write and print expanded text for debugging
    sample_text.write_text("../output/01_text_after_expansion.txt")

    # Initialize Normalisation object
    normalisation = Normalisation(lexicon.df)

    # Normalise word segmentation
    sample_text.text = normalisation.word_segmentation(sample_text.text)

    # Write and print expanded text for debugging
    sample_text.write_text("../output/02_text_after_segmentation.txt")

    # Normalise text
    sample_text.text = normalisation.normalise_text(sample_text.text)

    # Write and print expanded text for debugging
    sample_text.write_text("../output/03_text_after_normalisation.txt")


if __name__ == "__main__":
    main()