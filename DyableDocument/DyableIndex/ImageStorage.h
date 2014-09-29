#include "../../DocumentDatabase.h"

// This class is responsible for storing all the image
// information. This relates to storing the image url
// as a text string. Note that each image url is not
// checked for redundancy. It's not likely that the
// same image with the same caption is repeated on many 
// pages. Any redundancy will be removed during lookup.
class CImageStorage {

	// defines the number of images stored per compression block
	static const int IMAGE_LOOKUP_SIZE = 65536;

	// true when the image set has been loaded, false otherwise
	bool m_image_set_loaded;
	// used to write the comp index for the image set
	CCompression m_image_lookup; 
	// used to store the image url text
	CCompression m_image_url_comp; 
	// stores the total number of images added
	_int64 m_image_num; 

	// indexes the current image url comp position
	void IndexCurrentImagePosition() {
		m_image_lookup.AskBufferOverflow(6);
		m_image_lookup.AddHitItem3Bytes(m_image_url_comp.CompBlockNum()); 
		m_image_lookup.AddHitItem3Bytes(m_image_url_comp.HitList().Size()); 
	}

	// this reads a one of the text strings from the image buffer
	// @param text - a buffer to store the text string
	// @param text_length - stores the length of the text string
	void RetrieveTextString(char text[], int &text_length) {
		text_length = 0; 
		m_image_url_comp.NextHit(*text); 

		while(*text != '\0') {
			text++; 
			text_length++; 
			m_image_url_comp.NextHit(*text); 
		}
	}

public:
	
	CImageStorage() {
		m_image_set_loaded = false;
	}

	// starts the file storage to start writing to
	void Initialize(int image_set) {
		m_image_lookup.Initialize(CUtility::ExtendString
			("GlobalData/Image/lookup", image_set), IMAGE_LOOKUP_SIZE * 6); 

		m_image_url_comp.Initialize(CUtility::ExtendString
			("GlobalData/Image/image_url", image_set)); 

		m_image_num = 0; 
	}

	// loads the index in order to retrieve a certain image
	// @param image_set - the image set belonging to a particular client
	// @param index - the image index
	void LoadImageStorage(int image_set) {

		if(m_image_set_loaded)return;
		m_image_set_loaded = true;
		m_image_url_comp.LoadIndex(CUtility::ExtendString
			("GlobalData/Image/image_url", image_set)); 

		m_image_lookup.LoadIndex(CUtility::ExtendString
			("GlobalData/Image/lookup", image_set)); 
	}

	// returns the total number of images stored
	inline _int64 ImagesStored() {
		return m_image_num; 
	}

	// retrieves a particular image url from storage
	// @param doc_url - a buffer to store the document url
	// @param doc_length - the length of the document url
	// @param image_url - a buffer to store the image url
	// @param image_length - the length of the image url
	// @param index - the index of the image to retrieve
	// @param caption - a buffer to store the image caption
	// @param caption_length - the length of the image caption
	void RetrieveImage(char doc_url[], int &doc_length,
		char image_url[], int &image_length, const _int64 index,
		char caption[], int &caption_length) {
	
		SHitPosition lookup_pos;
		lookup_pos.comp_block = (int)(index / IMAGE_LOOKUP_SIZE);
		lookup_pos.hit_offset = (int)(index % IMAGE_LOOKUP_SIZE) * 6;
		m_image_lookup.SetCurrentHitPosition(lookup_pos);

		SHitPosition image_pos; 
		m_image_lookup.NextHit((char *)&image_pos.comp_block, 3); 
		m_image_lookup.NextHit((char *)&image_pos.hit_offset, 3); 
		m_image_url_comp.SetCurrentHitPosition(image_pos); 

		RetrieveTextString(doc_url, doc_length);
		RetrieveTextString(image_url, image_length);
		RetrieveTextString(caption, caption_length);
	}

	// adds a image url to the set
	// @param doc_url - the buffer containing the document url
	// @param doc_length - the length of the document url in bytes
	// @param image_url - the buffer containing the image url
	// @param image_length - the length of the image url in bytes
	// @param caption - the buffer containing the caption associated with the image
	// @param caption_length - the size of the caption in bytes
	void AddImageURL(char doc_url[], int doc_length,
		char image_url[], int image_length, char caption[],
		int caption_length) {

		IndexCurrentImagePosition(); 
		m_image_url_comp.AddToBuffer(doc_url, doc_length); 
		m_image_url_comp.AskBufferOverflow(1); 
		m_image_url_comp.HitList().PushBack('\0'); 

		m_image_url_comp.AddToBuffer(image_url, image_length); 
		m_image_url_comp.AskBufferOverflow(1); 
		m_image_url_comp.HitList().PushBack('\0'); 

		m_image_url_comp.AddToBuffer(caption, caption_length); 
		m_image_url_comp.AskBufferOverflow(1); 
		m_image_url_comp.HitList().PushBack('\0'); 
		m_image_num++; 
	}

	// finishes the current image set
	~CImageStorage() {
		m_image_url_comp.FinishCompression(); 
		m_image_lookup.FinishCompression();
	}
}; 
