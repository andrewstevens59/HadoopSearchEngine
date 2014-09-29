
var keyword_select = new Array();
var keyword_select_size = 0;

function SKeyword(keyword_id, occur) { 
    this.keyword_id = keyword_id;
    this.occur = occur;
};

var excerpt_select;
var global_occur;
var keyword_occur;
var options;
var excerpt_num = 0;
var red_doc_set = doc_set;
var red_node_set = node_set;
var caption_text = query_text;
var phrase_pos;
var curr_excerpt = 0;
var min_term_num = 3;
var is_click_box_set = false;

function setClickBox() {
    is_click_box_set = true;
}

function clearClickBox() {
    is_click_box_set = false;
}

window.onscroll = scroll;
function scroll() {
    if (getYOffset() >= window.scrollMaxY && excerpt_num < red_doc_set.length) {
        loadXMLDoc(red_doc_set[excerpt_num], caption_text, red_node_set[excerpt_num], true, term_weight[excerpt_num]);
        excerpt_num++;
    }
}

// this is simply a shortcut for the eyes and fingers
function $(id) {
    return document.getElementById(id);
}

function SSentence(excerpt_id, sentence, keyword_num, phrase_snippet) {
    this.excerpt_id = excerpt_id;
    this.sentence = sentence;
    this.keyword_num = keyword_num;
    this.phrase_snippet = phrase_snippet;
};

function SOverview(phrase, sentence, keyword_num, phrase_snippet) {
    this.phrase = phrase;
    this.sentence = sentence;
    this.keyword_num = keyword_num;
    this.phrase_snippet = phrase_snippet;
};

function SExcerpt(summary, excerpt, overview_set, excerpt_id) {
    this.summary = summary;
    this.excerpt = excerpt;
    this.overview_set = overview_set;
    this.excerpt_id = excerpt_id;
};

function SPhrasePos(pos, phrase, fill_len) {
    this.pos = pos;
    this.phrase = phrase;
    this.fill_len = fill_len;
};

var string_cache = new Array();
var excerpt_set = new Array();
var phrase_set = new Array();
var phrase_num = new Array();
var sentence_set = new Array();
var excerpt_remap = new Array();
var excerpt_lookup = new Array();
var doc_set_map = new Array();
var excerpt_active = new Array();
var extra_term_set = new Array();
var phrase_size = new Array();
var phrase_score = new Array();
var full_phrase = new Array();

function getYOffset() {
    var pageY;
    if (typeof (window.pageYOffset) == 'number') {
        pageY = window.pageYOffset;
    }
    else {
        pageY = document.body.scrollTop;
    }
    return pageY;
}

function stemPhrase(phrase) {

    var start = phrase.indexOf("[");
    var end = phrase.indexOf("]");

    return phrase.substring(start + 1, end);
}

function addExcerptData(phrase_snippet, phrase, indv_phrase_num, sentence,
    keyword_num, excerpt_id, is_phase_set, term_weight) {

    var full = phrase.replace("[", "");
    full = full.replace("]", "");
    phrase = stemPhrase(phrase);

    if (phrase_num[phrase] == undefined) {
        phrase_set[phrase] = new Array();
        sentence_set[phrase] = new Array();
        phrase_num[phrase] = 0;
        phrase_score[phrase] = 0;
        phrase_size[phrase] = indv_phrase_num;
        full_phrase[phrase] = full;
    }

    excerpt_set[excerpt_id].overview_set.push(new SOverview(phrase, sentence, keyword_num, phrase_snippet));

    if (sentence.length < 4) {
        return;
    }

    if (sentence_set[phrase][sentence] != undefined) {
        return;
    }

    sentence_set[phrase][sentence] = 0;
    phrase_set[phrase][phrase_num[phrase]++] = new SSentence(excerpt_id, sentence, keyword_num, phrase_snippet);

    if (is_phase_set[phrase] == undefined) {
        is_phase_set[phrase] = true;
        phrase_score[phrase] += term_weight;
    }

    if (phrase_num[phrase] > 40) {
        phrase_score[phrase] = 0;
    }
}

function loadXMLDoc(doc_id, query, node_id, is_keywords, term_weight) {

    var excerpt_id;

    if (doc_set_map[doc_id] != undefined) {
        excerpt_id = doc_set_map[doc_id];
    } else {
        excerpt_id = curr_excerpt++;
    }

    var xmlhttp;
    if (window.XMLHttpRequest) {// code for IE7+, Firefox, Chrome, Opera, Safari
        xmlhttp = new XMLHttpRequest();
    }
    else {// code for IE6, IE5
        xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
    }

    xmlhttp.onreadystatechange = function () {
        if (xmlhttp.readyState == 4 && xmlhttp.status == 200) {

            var text = xmlhttp.responseText;

            var offset = 0;
            var start = 0;
            var currentTagTokens = new Array();
            for (var i = 0; i < text.length; i++) {
                if (text.charAt(i) == '`') {
                    currentTagTokens[offset++] = text.substring(start, i);
                    start = i + 1;
                }
            }

            currentTagTokens[offset++] = text.substring(start, text.length);

            var excerpt = node_id.toString() + " " + cluster_keyword_set.toLowerCase() + " " + "<div style=\"line-height: 1.5em;\">" + currentTagTokens[0] + "</div>";
            var summary = "<font size=\"2\">" + currentTagTokens[currentTagTokens.length - 1] + "</font>";

            doc_set_map[doc_id] = excerpt_id;
            excerpt_remap[excerpt_id] = excerpt_id;
            excerpt_lookup[excerpt_id] = excerpt_id;

            var is_phase_set = new Array();
            excerpt_set[excerpt_id] = new SExcerpt(summary, excerpt, new Array(), excerpt_id);
            excerpt_active[excerpt_id] = true;

            for (var i = 1; i < currentTagTokens.length - 1; i += 5) {
                var phrase_snippet = currentTagTokens[i];
                var phrase = currentTagTokens[i + 1];
                var indv_phrase_num = parseInt(currentTagTokens[i + 2]);
                var sentence = currentTagTokens[i + 3];
                var keyword_num = parseInt(currentTagTokens[i + 4]);

                addExcerptData(phrase_snippet, phrase, indv_phrase_num,
                    sentence, keyword_num, excerpt_id, is_phase_set, term_weight);
            }

            if ((excerpt_id % 5) == 0) {
                createSummaryBox(30, false, "right_summary", 2);
            }

            rankExcerpts((excerpt_id % 5) == 0);
        }
    }

    var text = "http://" + domain_name + "/cgi-bin/DocumentQuery?doc=";
    text += doc_id;
    text += "&q=";
    text += query.toLowerCase(); 
    text += "&node=";
    text += node_id;
    text += "&excerpt=";
    text += excerpt_id;

    if (is_keywords == true) {
        text += "&keywords=";
        text += cluster_keyword_set.toLowerCase(); 
    }

    xmlhttp.open("GET", text, true);
    xmlhttp.send();
}


function loadTextString1(link_id, keyword_id) {
    var xmlhttp;
    if (window.XMLHttpRequest) {// code for IE7+, Firefox, Chrome, Opera, Safari
        xmlhttp = new XMLHttpRequest();
    }
    else {// code for IE6, IE5
        xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
    }
    xmlhttp.onreadystatechange = function () {
        if (xmlhttp.readyState == 4 && xmlhttp.status == 200) {

            var text = "<font size=\"3\">" + xmlhttp.responseText.replace("^", " ") + "</font>";
            document.getElementById(link_id).innerHTML = text;
            document.getElementById(link_id).href = text;
            string_cache[keyword_id] = xmlhttp.responseText.replace(/\s+/g, "+");
        }
    }

    var text = "http://" + domain_name + "/cgi-bin/KeywordQuery?node=";
    text += keyword_id.toString();

    xmlhttp.open("GET", text, true);
    xmlhttp.send();
}

function loadTextString2(link_id, keyword_id) {
    var xmlhttp;
    if (window.XMLHttpRequest) {// code for IE7+, Firefox, Chrome, Opera, Safari
        xmlhttp = new XMLHttpRequest();
    }
    else {// code for IE6, IE5
        xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
    }
    xmlhttp.onreadystatechange = function () {
        if (xmlhttp.readyState == 4 && xmlhttp.status == 200) {

            setClusterTerm(link_id, keyword_id, xmlhttp.responseText);
        }
    }

    var text = "http://" + domain_name + "/cgi-bin/KeywordQuery?node=";
    text += keyword_id.toString();

    xmlhttp.open("GET", text, true);
    xmlhttp.send();
}

function setClusterTerm(link_id, keyword_id, inner_text) {

    document.getElementById(link_id).innerHTML = "<b> > " + inner_text.replace("^", " ") + "</b>";

    var tag = "nav" + keyword_select_size.toString();
    document.getElementById(tag).innerHTML = "[Search Fully]";
    document.getElementById(tag).href = "DyableQuery?q=" + inner_text;
}

InitDragDrop();

function loadNextExcerpt() {
    if (excerpt_num < red_doc_set.length) {
        loadXMLDoc(red_doc_set[excerpt_num], caption_text, red_node_set[excerpt_num], true, term_weight[excerpt_num]);
        excerpt_num++;
    }

    setTimeout("loadNextExcerpt()", 1000);
}

function InitDragDrop() {
    document.onmousedown = OnMouseDown;
    document.onmouseup = OnMouseUp;
    document.onmouseover = OnMouseOver;
    document.onmouseout = OnMouseOut;

    var height = screen.height * 0.35;
    document.getElementById("disp_excerpt").style.height = height;
    document.getElementById("summary_box").style.height = height;

    global_occur = new Array();

    for (var i = 0; i < global_keyword_set.length; i++) {
        global_occur[global_keyword_set[i].toString()] = global_keyword_set_occur[i];
    }

    for (var i = 0; i < Math.min(40, red_doc_set.length); i++) {
        loadXMLDoc(red_doc_set[excerpt_num], caption_text, red_node_set[excerpt_num], true, term_weight[excerpt_num]);
        excerpt_num++;
    }

    CompileResults(0);
    setTimeout("loadNextExcerpt()", 2000);
}

function processKeywords() {

    var tok_text = document.getElementById("addkeywords").value.replace(/\s+/g, "+") + "+";
    cluster_keyword_set += tok_text;
    extra_term_set[tok_text] = 0;

    document.getElementById("addkeywords").value = "";
    for (var i = 0; i < excerpt_set.length; i++) {
        document.getElementById(i.toString()).style.display = "none";
    }

    excerpt_set = new Array();
    phrase_set = new Array();
    phrase_num = new Array();
    sentence_set = new Array();
    excerpt_remap = new Array();
    excerpt_lookup = new Array();
    doc_set_map = new Array();
    excerpt_active = new Array();

    curr_excerpt = 0;
    excerpt_num = 0;
    for (var i = 0; i < Math.min(40, red_doc_set.length); i++) {
        loadXMLDoc(red_doc_set[excerpt_num], caption_text, red_node_set[excerpt_num], true, term_weight[excerpt_num]);
        excerpt_num++;
    }

    setTimeout("drawSummaryBox();", 1000);
}

function showPassage(div) {
    div.style.backgroundColor = "#E0ECF8";
    excerpt_select = div;
}

function hidePassage(div) {
    div.style.backgroundColor = "white";
}

function closeBox() {
    document.getElementById("addkeywords").value = getSelText();
    document.getElementById("cover").style.display = "none";
    document.getElementById("disp_excerpt").style.display = "none";
}

function drawClose() {
    return "<a href=\"Close\" onclick=\"closeBox();return false;\" style=\"float: right;\">[Close]</a>";
}

function sortExcerpts(score_set) {

    for (var i = 0; i < score_set.length; i++) {
        for (var j = i + 1; j < score_set.length; j++) {

            if (excerpt_active[i] == false || excerpt_active[j] == false) {
                continue;
            }

            if (score_set[j] > score_set[i]) {
                var temp1 = excerpt_set[i];
                var temp2 = score_set[i];
                var temp3 = excerpt_remap[i];
                var temp4 = doc_set_map[i];
                var temp5 = excerpt_active[i];
                excerpt_set[i] = excerpt_set[j];
                excerpt_set[j] = temp1;

                score_set[i] = score_set[j];
                score_set[j] = temp2;

                excerpt_remap[i] = excerpt_remap[j];
                excerpt_remap[j] = temp3;

                doc_set_map[i] = doc_set_map[j];
                doc_set_map[j] = temp4;

                excerpt_active[i] = excerpt_active[j];
                excerpt_active[j] = temp5;
            }
        }
    }
}

function calculateExcerptScore(score_set, weight_set, max_occur) {

    for (var j = 0; j < excerpt_set.length; j++) {

        if (excerpt_active[j] == false) {
            continue;
        }

        var score = 0;
        var curr_phrase = null;
        for (var i = 0; i < excerpt_set[j].overview_set.length; i++) {
            var phrase = excerpt_set[j].overview_set[i].phrase;

            if (excerpt_set[j].overview_set[i].sentence.length < 4) {
                continue;
            }

            if (phrase != curr_phrase) {
                score += Math.max(Math.min(max_occur, weight_set[phrase] - 2), 0) << Math.min(phrase_size[phrase], 2);
                curr_phrase = phrase;
            }

        }

        score_set[j] = score;
    }
}

function updatePhraseScore(score_set, weight_set, top_num) {

    for (var j = 0; j < top_num; j++) {

        if (excerpt_active[j] == false) {
            continue;
        }

        var curr_phrase = null;
        for (var i = 0; i < excerpt_set[j].overview_set.length; i++) {
            var phrase = excerpt_set[j].overview_set[i].phrase;

            if (excerpt_set[j].overview_set[i].sentence.length < 4) {
                continue;
            }

            if (phrase != curr_phrase) {
                weight_set[phrase] += score_set[j];
                curr_phrase = phrase;
            }

        }
    }
}

function rankExcerpts(is_sort) {

    var score_set = new Array();
    var weight_set = phrase_score;

    if (is_sort == true) {
        calculateExcerptScore(score_set, weight_set, 5);
        sortExcerpts(score_set);

        updatePhraseScore(score_set, weight_set, Math.min(excerpt_set.length, 10));
        calculateExcerptScore(score_set, weight_set, 6);
        sortExcerpts(score_set);
    }

    for (var j = 0; j < excerpt_set.length; j++) {
        if (excerpt_active[j] == true) {
            document.getElementById(j.toString()).innerHTML = "<div onclick=\"handleExcerpt(" + j.toString() + ");\">" + excerpt_set[j].summary + "</div>";
            document.getElementById(j.toString()).style.display = "block";
        }

        excerpt_lookup[excerpt_remap[j]] = j;
    }
}

function ExcerptSnippet(excerpt_id, sentence) {

    var excerpt = excerpt_set[excerpt_id].excerpt;
    var start = excerpt.indexOf(sentence.sentence);
    var end = start + sentence.sentence.length;

    var text = "<font size=\"3\">";
    text += excerpt.substring(0, start);
    text += "<a name=\"blah" + "\">";
    text += "<u style=\"background-color:#E0ECF8;\">[" + excerpt.substring(start, end) + "]</u>";
    text += excerpt.substring(end, excerpt.length);
    text += "</i></b></font>";

    return text;
}

function removeHTMLTags(sentence, tag_type) {

    var offset;

    if (tag_type == true) {
        offset = sentence.indexOf("<");

        var tag = sentence.substring(offset, offset + 3);
        if (tag == "<FO" || tag == "<B>" || tag == "</F" || tag == "</B") {
            return sentence.substring(0, offset) + "<" + removeHTMLTags(sentence.substring(offset + 1, sentence.length), true);
        }

    } else {
        offset = sentence.indexOf(">");
    }

    if (offset < 0) {
        return sentence;
    }

    if (tag_type == true) {
        return sentence.substring(0, offset) + removeHTMLTags(sentence.substring(offset, sentence.length), false);
    }

    return removeHTMLTags(sentence.substring(offset + 1, sentence.length), true);
}

function renderPassage(sentence, phrase, phrase_snippet, is_highlight, sent_offset) {

    var offset = sentence.indexOf(phrase_snippet);

    if (offset < 0) {
        return sentence;
    }

    var text = sentence.substring(0, offset);

    if (is_highlight == false) {
        var snippet = "";
        if (phrase_num[phrase] > 1) {
            snippet += "<a href=\"" + Math.random() + "\" onclick=\"return jumpToExcerpt('" + phrase + "');\">";
        }

        snippet += "<font color=\"blue\"><b>" + phrase_snippet + "</b></font>";
        if (phrase_num[phrase] > 1) {
            snippet += "</a>";
        }

        phrase_pos.push(new SPhrasePos(sent_offset + offset, snippet, phrase_snippet.length));
        for (var i = 0; i < phrase_snippet.length; i++) {
            text += "-";
        }

    } else {
        text += "<FONT COLOR=\"#FF8000\"><B>";
        text += phrase_snippet;
        text += "</B></FONT>";
    }

    return text + renderPassage(sentence.substring(offset + phrase_snippet.length, sentence.length),
         phrase, phrase_snippet, is_highlight, sent_offset + offset + phrase_snippet.length);
}

function highlightKeywords(excerpt_id, passage) {

    phrase_pos = new Array();
    var pharse_set = new Array();
    var title = passage.substring(0, passage.indexOf("</a>"));
    passage = passage.substring(passage.indexOf("</a>"), passage.length);

    for (var i = 0; i < excerpt_set[excerpt_id].overview_set.length; i++) {
        var phrase = excerpt_set[excerpt_id].overview_set[i].phrase;
        var phrase_snippet = excerpt_set[excerpt_id].overview_set[i].phrase_snippet;

        if (pharse_set[phrase_snippet] != undefined) {
            continue;
        }

        pharse_set[phrase_snippet] = true;
        passage = renderPassage(passage, phrase, phrase_snippet, false, 0);
    }

    for (var i = 0; i < phrase_pos.length; i++) {
        for (var j = i + 1; j < phrase_pos.length; j++) {
            if (phrase_pos[j].pos < phrase_pos[i].pos) {
                var temp = phrase_pos[i];
                phrase_pos[i] = phrase_pos[j];
                phrase_pos[j] = temp;
            }
        }
    }

    var full_text = "";
    var before = 0;
    for (var i = 0; i < phrase_pos.length; i++) {
        full_text += passage.substring(before, phrase_pos[i].pos);
        full_text += phrase_pos[i].phrase;
        before = phrase_pos[i].pos + phrase_pos[i].fill_len;
    }

    return title + full_text;
}

function displayExcerpt(id, k, is_phrase_set) {

    var sentence;
    var excerpt_id;

    if (is_phrase_set == true) {
        sentence = phrase_set[id][k];
        excerpt_id = excerpt_lookup[sentence.excerpt_id];
    } else {
        sentence = excerpt_set[id].overview_set[k];
        excerpt_id = id;
    }

    var text = ExcerptSnippet(excerpt_id, sentence);
    text = highlightKeywords(excerpt_id, text);

    document.getElementById("summary_box").style.top = getYOffset();
    document.getElementById("disp_excerpt").style.top = getYOffset();
    document.getElementById("disp_excerpt").innerHTML = text;
    document.getElementById("disp_excerpt").style.display = "block";
    document.getElementById("cover").style.display = "block";

    var before = getYOffset();
    window.location = "#blah";
    window.scrollTo(0, before);
}

function rankSentences(phrase, num, disp_phrase_set) {

    var set = new Array();
    var sentence_set = new Array();
    var offset = 0;

    var text = "";
    for (var j = 0; j < phrase_num[phrase]; j++) {
        var max = -1;
        var id = -1;

        for (var k = 0; k < phrase_num[phrase]; k++) {
            var sentence = phrase_set[phrase][k];

            if (set[k] != undefined) {
                continue;
            }

            if (sentence.keyword_num > max) {
                max = sentence.keyword_num;
                id = k;
            }
        }

        if (id < 0) {
            return text;
        }

        set[id] = true;
        var temp = phrase_set[phrase][id];

        if (sentence_set[temp.sentence] == undefined) {
            var sentence = removeHTMLTags(temp.sentence, true);
            sentence_set[temp.sentence] = true;

            if (sentence.length < 4) {
                continue;
            }

            if (disp_phrase_set == false) {
                text += "<div onmouseover=\"showPassage(this);\" onmouseout=\"hidePassage(this);\" onclick=\"return displayExcerpt('" + phrase + "'," + id.toString() + ", true);\">";
            } else {
                text += "<div onmouseover=\"setClickBox();showPassage(this);\" onmouseout=\"clearClickBox();hidePassage(this);\" onclick=\"jumpToExcerpt('" + phrase + "');return displayExcerpt('" + phrase + "'," + id.toString() + ", true);\">";
            }

            text += "<ul><li>";
            text += renderPassage(sentence, phrase, temp.phrase_snippet, true, 0) + "</a>";
            text += "</ul></div>";

            if (++offset >= num) {
                return text;
            }
        }
    }

    return text;
}

function createSummaryBox(header_num, draw_box, box_div, sent_num) {

    var summary_text = "";
    var keyword_text = "";
    var set = new Array();
    var offset = 0;
    var id;

    while(true) {
        var max = 0;
        for (var i in phrase_num) {
            if (set[i] != undefined) {
                continue;
            }

            var score = phrase_num[i] + (Math.min(phrase_size[i], 2) << 8);

            if (score > max) {
                max = score;
                id = i;
            }
        }

        if (max == 0 || phrase_num[id] < 2) {
            break;
        }

        set[id] = true;
        var added_text = "";

        if (draw_box == true) {
            added_text += "<h3><a onclick=\"return jumpToExcerpt('" + id + "');\" href=\"" + id + "\">" + full_phrase[id] + "</a>";
        } else {
            added_text += "<h3><a onmouseover=\"setClickBox();\" onmouseout=\"clearClickBox();\" onclick=\"return jumpToExcerpt('" + id + "');\" href=\"" + id + "\">" + full_phrase[id] + "</a>";
        }
        
         
        added_text += " (" + phrase_num[id] + ")";
        var sentence_set = rankSentences(id, sent_num, draw_box == false);
        offset++;

        if (offset < header_num) {
            summary_text += added_text + "</h3>";
            if (draw_box == true) {
                summary_text += "<div style=\"overflow: auto; height: 120px; border: 1px solid blue;\">";
            }

            summary_text += sentence_set;

            if (draw_box == true) {
                summary_text += "</div>";
            }
        }

        var toks = id.split(" ");
        var tok_text = "";
        for (var i = 0; i < toks.length; i++) {
            if (toks[i].length > 0) {
                tok_text += toks[i].toLowerCase() + "+";
            }
        }
    }

    if (keyword_text.length > 0) {
        summary_text = keyword_text + "<P><hr style=\"color:blue; height:1px;\">" + summary_text;
    }

    if (draw_box == true) {
        document.getElementById(box_div).style.top = getYOffset();
        document.getElementById(box_div).innerHTML = drawClose() + "<center><h1>Summary</h1></center>" + summary_text;
    } else {
        document.getElementById(box_div).innerHTML = "<center><h1>Summary</h1></center>" + summary_text;
    }
}

function drawSummaryBox() {

    createSummaryBox(50, true, "summary_box", 100);
    document.getElementById("cover").style.display = "block";
    document.getElementById("summary_box").style.display = "block";
    document.getElementById("summary_box").scrollTop = 0;

    return false;
}

function jumpToExcerpt(phrase) {
    
    var text = drawClose() + "<h1>";
    text += full_phrase[phrase] + "&nbsp;<font size=\"2\"><a href=\"DyableQuery?q=" + phrase.replace(/\s+/g, "+") + "\">[Search Fully]</a></font></h1><font size=\"3\">";
    text += rankSentences(phrase, phrase_num[phrase], false);
    text += "</font>";

    document.getElementById("summary_box").style.top = getYOffset();
    document.getElementById("disp_excerpt").style.top = getYOffset();
    document.getElementById("summary_box").innerHTML = text;
    document.getElementById("summary_box").style.display = "block";
    document.getElementById("summary_box").scrollTop = 0;
    document.getElementById("cover").style.display = "block";

    return false;
}

function ExpandSentence(excerpt_id) {

    var curr_set = new Array();
    var num = new Array();
    var set = new Array();
    var offset = 0;

    var text = "";
    var curr_phrase = null;
    var excerpt = excerpt_set[excerpt_id];
    var sentence_avail = new Array();

    for (var i = 0; i < excerpt.overview_set.length; i++) {
        var phrase = excerpt.overview_set[i].phrase;
        var sentence = excerpt.overview_set[i].sentence;
        var phrase_snippet = excerpt.overview_set[i].phrase_snippet;

        if (sentence.length < 4) {
            continue;
        }

        if (phrase_num[phrase] < min_term_num) {
            continue;
        }

        sentence = removeHTMLTags(sentence, true);
        sentence = renderPassage(sentence, phrase, phrase_snippet, true, 0);

        if(sentence_avail[sentence] != undefined) {
            continue;
        }

        if (phrase != curr_phrase) {
            curr_set[offset] = text;
            num[offset] = phrase_num[phrase];
            text = "<h3><a href=\"" + phrase + "\" onclick=\"return jumpToExcerpt('" + phrase + "');\">" + full_phrase[phrase] + "</a> (" + phrase_num[phrase] + ")</h3>";
            sentence_avail = new Array();
            curr_phrase = phrase;
            offset = offset + 1;
        }

        sentence_avail[sentence] = 0;
        text += "<ul><li><div onmouseover=\"showPassage(this);\" onmouseout=\"hidePassage(this);\" onclick=\"return displayExcerpt(" + excerpt_id.toString() + "," + i.toString() + ", false);\"><font size=\"3\">" + sentence + "</font></div></ul><p>";
    }
    
    curr_set[offset] = text;
    text = drawClose();

    for (var i = 1; i < curr_set.length; i++) {
        var id = 0;
        var max = 0;
        for (var j = 1; j < curr_set.length; j++) {

            if (set[j] != undefined) {
                continue;
            }

            if (num[j-1] > max) {
                max = num[j-1];
                id = j;
            }
        }

        set[id] = true;
        text += curr_set[id];
    }


    document.getElementById("summary_box").style.top = getYOffset();
    document.getElementById("disp_excerpt").style.top = getYOffset();
    document.getElementById("summary_box").innerHTML = text;
    document.getElementById("summary_box").style.display = "block";
    document.getElementById("summary_box").scrollTop = 0;
    document.getElementById("cover").style.display = "block";

    return false;
}


function ExpandExcerpt(excerpt_id) {

    var pharse_set = new Array();
    var text = excerpt_set[excerpt_id].excerpt;

    document.getElementById("disp_excerpt").style.top = getYOffset();
    document.getElementById("disp_excerpt").innerHTML = "<font size=\"3\">" + highlightKeywords(excerpt_id, text) + "</font>";
    document.getElementById("disp_excerpt").style.display = "block";
    document.getElementById("disp_excerpt").scrollTop = 0;
    window.scrollTo(0, getYOffset());

    return false;
}

function handleExcerpt(id) {
    ExpandSentence(id);
    ExpandExcerpt(id);
}

function getSelText() {
    var txt = '';
    if (window.getSelection) {
        txt = window.getSelection();
    }
    else if (document.getSelection) {
        txt = document.getSelection();
    }
    else if (document.selection) {
        txt = document.selection.createRange().text;
    }

    return txt;
}

function OnMouseDown(e) {
    // IE is retarded and doesn't pass the event object
    if (e == null)
        e = window.event;

    // IE uses srcElement, others use target
    var target = e.target != null ? e.target : e.srcElement;

    if (is_click_box_set != true) {
        closeBox();
    }

}

function OnMouseMove(e) {
}

function OnMouseOut(e) {
}

function OnMouseOver(e) {
}

function OnMouseUp(e) {
}

function CompileResults(nav_link) {

    for (var i = 0; i < 20; i++) {
        var tag = "exp" + i.toString();
        document.getElementById(tag).innerHTML = "";
    }

    for (var i = 0; i < 6; i++) {
        tag = "nav" + i.toString();
        document.getElementById(tag).innerHTML = "";
    }
    
    FindKeywordOccurrence();
    FindMaxKeyword();

    for (var i = 0; i < excerpt_set.length; i++) {
        document.getElementById(i.toString()).style.display = "none";
        excerpt_active[i] = false;
    }

    for (var i = 0; i < Math.min(keyword_select_size, 6); i++) {
        var tag = "nav" + i.toString();
        if (string_cache[keyword_select[i]] == null) {
            loadTextString2(tag, options[i]);
        } else {
            setClusterTerm(tag, keyword_select[i], string_cache[keyword_select[i]]);
        }
    }

    for (var i = 0; i < Math.min(options.length, 20); i++) {
        var tag = "exp" + i.toString();
        if (string_cache[options[i]] == null) {
            loadTextString1(tag, options[i]);
        } else {
            document.getElementById(tag).innerHTML = string_cache[options[i]].replace("^", " ");
        }
    }
}

function Navigate(nav_link) {

    if (nav_link >= keyword_select_size) {
        return true;
    }

    keyword_select_size--;
    keyword_select.splice(nav_link, 1);

    CompileResults(nav_link);

    if (keyword_select_size == 0) {
        caption_text = query_text;
        excerpt_num = Math.min(40, red_doc_set.length);
        for (var i = 0; i < excerpt_num; i++) {
            loadXMLDoc(red_doc_set[i], caption_text, red_node_set[i], true, term_weight[i]);
        }

        return false;
    }

    caption_text = "";
    for (var i = 0; i < Math.min(keyword_select_size, 6); i++) {
        caption_text += string_cache[keyword_select[i]] + "+";
    }

    excerpt_num = Math.min(10, red_doc_set.length);
    for (var i = 0; i < excerpt_num; i++) {
        loadXMLDoc(red_doc_set[i], caption_text, red_node_set[i], true, term_weight[i]);
    }

    return false;
}

function Contents(nav_link) {

    keyword_select[keyword_select_size++] = options[nav_link];

    CompileResults(nav_link);

    caption_text = "";
    for (var i = 0; i < Math.min(keyword_select_size, 6); i++) {
        caption_text += string_cache[keyword_select[i]] + "+";
    }

    var set = "";
    excerpt_num = Math.min(10, red_doc_set.length);
    for (var i = 0; i < excerpt_num; i++) {
        set += red_doc_set[i].toString() + " ";
        loadXMLDoc(red_doc_set[i], caption_text, red_node_set[i], true, term_weight[i]);
    }

    return false;
}

function FindMaxKeyword() {

    options = new Array();
    var offset = 0;

    for (var j = 0; j < Math.min(20, keyword_occur.length); j++) {
        var max = new SKeyword(0, 0);
        var key = null;

        for (var keyword in keyword_occur) {

            var occur = keyword_occur[keyword].occur;
            if (global_occur[keyword] != null) {
                occur *= global_occur[keyword];
            }

            if (occur >= max.occur) {
                max = keyword_occur[keyword];
                max.occur = occur;
                key = keyword;
            }
        }

        if (keyword_occur[key].occur > 1) {
            options[offset++] = max.keyword_id;
        }

        keyword_occur[key].occur = 0;
    }
}

function FindKeywordOccurrence() {

    red_doc_set = new Array();
    red_node_set = new Array();
    keyword_occur = new Array();
    var keyword_map = new Array();

    for (var j = 0; j < keyword_select_size; j++) {
        keyword_map[keyword_select[j].toString()] = true;
    }

    var offset = 0;
    for (var i = 0; i < keyword_set.length; i++) {
        var count = 0;
        for (var k = 0; k < keyword_set[i].length; k++) {
            var key = keyword_set[i][k].toString();
            if (keyword_map[key] != null) {
                count++;
            }
        }

        if (count != keyword_select_size) {
            continue;
        }

        red_node_set[offset] = node_set[i];
        red_doc_set[offset++] = doc_set[i];

        for (var k = 0; k < keyword_set[i].length; k++) {
            var key = keyword_set[i][k].toString();
            if (keyword_map[key] != null) {
                continue;
            }

            if (keyword_occur[key] == null) {
                keyword_occur[key] = new SKeyword(keyword_set[i][k], 1);
            } else {
                keyword_occur[key].occur++;
            }
        }
    }
}
