#include "../NameServer.h"

// This is repsonsible for finding a set of alternative spellings 
// for a given word. This is done by consulting the global lexicon
// and using the base word dictionary to find a high occuring 
// alternative spelling.
class CAlternateSpelling {

	// This is used to find alternate spellings
	CSpellCheck m_spell_check;

public:

	CAlternateSpelling() {
	}

	// This is used to initialize alternate spelling
	void Initialize() {
		m_spell_check.LoadSpellCheck();
	}

	// This finds an alternate spelling for a given text string
	char *SpellCheck(char *str, int &len) {

		static CString buff;
		buff.Reset();

		int word_end;
		int word_start;
		uChar match_score;

		str[len] = '\0';
		char *tok = strtok(str, " ^");
		for(int i=0; i<2; i++) {
			if(tok == NULL) {
				break;
			}

			int len = strlen(tok);
			char *word = m_spell_check.FindClosestMatch(tok, len, match_score);

			if(match_score < 3) {
				word[len] = '\0';
				buff += word;
			} else {
				buff += tok;
			}

			buff += " ";
			tok = strtok(NULL, " ^");
		}

		buff.PopBack();
		len = buff.Size();
		return buff.Buffer();
	}
};