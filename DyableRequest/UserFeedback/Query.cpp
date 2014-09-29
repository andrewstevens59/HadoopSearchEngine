#include "../../MapReduce.h"

int main(int argc, char *argv[])
{
	cout<<"Content-type: text/html\n\n";
	CUtility::Initialize();
	char *len = getenv("CONTENT_LENGTH");
	int length = (len == NULL) ? 0 : atoi(len);
	char *temp = getenv("QUERY_STRING");

	if(temp != NULL && strlen(temp) > 0) {
		length = strlen(getenv("QUERY_STRING"));
	}

	cout<<"<h1><center>Thank You For Your Feedback</center></h1>";
	cout<<"<meta http-equiv=\"REFRESH\" content=\"1;url=../Synopsis.html\">";

	if(length > 4096 || length < 20) {
		return 0;
	}

	char *data = new char[length + 1];

	if(temp != NULL && strlen(temp) > 0) {
		strcpy(data, temp);
	} else if(length > 0) {
		gets(data);
	} else {
		data[0] = '\0';
	}

	time_t t;
	time(&t);
	FILE *fp = fopen("user_feedback.txt", "a+");

	for(int i=0; i<length; i++) {
		if(CUtility::AskEnglishCharacter(data[i])) {
			fprintf(fp, "%c", data[i]);
		} else {
			fprintf(fp, " ");
		}
	}

	if(fp != NULL) {
		fprintf(fp, "\n %s\n*******************************\n", asctime(localtime(&t)));
		fclose(fp);
	}

	delete(data);

    return 0;
}