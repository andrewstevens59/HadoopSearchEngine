// DyableCommandView.h : CDyableCommandView ���O������
//


#pragma once


class CDyableCommandView : public CView
{
protected: // �ȱq�ǦC�ƫإ�
	CDyableCommandView();
	DECLARE_DYNCREATE(CDyableCommandView)

// �ݩ�
public:
	CDyableCommandDoc* GetDocument() const;

// �@�~
public:

// �мg
	public:
	virtual void OnDraw(CDC* pDC);  // �мg�H�yø���˵�
virtual BOOL PreCreateWindow(CREATESTRUCT& cs);
protected:
	virtual BOOL OnPreparePrinting(CPrintInfo* pInfo);
	virtual void OnBeginPrinting(CDC* pDC, CPrintInfo* pInfo);
	virtual void OnEndPrinting(CDC* pDC, CPrintInfo* pInfo);

// �{���X��@
public:
	virtual ~CDyableCommandView();
#ifdef _DEBUG
	virtual void AssertValid() const;
	virtual void Dump(CDumpContext& dc) const;
#endif

protected:

// ���ͪ��T�������禡
protected:
	DECLARE_MESSAGE_MAP()
};

#ifndef _DEBUG  // DyableCommandView.cpp ������������
inline CDyableCommandDoc* CDyableCommandView::GetDocument() const
   { return reinterpret_cast<CDyableCommandDoc*>(m_pDocument); }
#endif

