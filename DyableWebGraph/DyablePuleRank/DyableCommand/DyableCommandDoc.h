// DyableCommandDoc.h : CDyableCommandDoc ���O������
//


#pragma once

class CDyableCommandDoc : public CDocument
{
protected: // �ȱq�ǦC�ƫإ�
	CDyableCommandDoc();
	DECLARE_DYNCREATE(CDyableCommandDoc)

// �ݩ�
public:

// �@�~
public:

// �мg
	public:
	virtual BOOL OnNewDocument();
	virtual void Serialize(CArchive& ar);

// �{���X��@
public:
	virtual ~CDyableCommandDoc();
#ifdef _DEBUG
	virtual void AssertValid() const;
	virtual void Dump(CDumpContext& dc) const;
#endif

protected:

// ���ͪ��T�������禡
protected:
	DECLARE_MESSAGE_MAP()
};


