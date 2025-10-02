import {
    _, Context, ContestModel, ObjectId, Counter, STATUS, getAlphabeticId, 
    formatSeconds, ScoreboardRow, db, UserModel, DocumentModel, PERM, diffArray, 
} from 'hydrooj';
import { ContestProblemListHandler } from 'hydrooj/src/handler/contest';
const {
    buildContestRule, isLocked, isDone
} = ContestModel;

const problemData = [
    { base: 1000, min: 300, perMin: 4 },
    { base: 1500, min: 450, perMin: 6 },
    { base: 2000, min: 600, perMin: 8 },
    { base: 2500, min: 750, perMin: 10 },
    { base: 3000, min: 900, perMin: 12 },
];
const FIRST_BLOOD_BONUS_RATE = 0.1; 

interface CodelinkJournal {
    rid: ObjectId;
    pid: number;
    score: number;
    status: number;
    time: number;
}
interface CodelinkDetail extends CodelinkJournal {
    naccept?: number;
    npending?: number;
    scorewf?: number; // score without first blood
}

const codelink = buildContestRule({
    TEXT: 'codelink',
    check: () => { },
    statusSort: { scoreSum: -1, acceptCount: -1, firstBloodCount: -1, time: 1 },
    submitAfterAccept: false,
    showScoreboard: (tdoc, now) => now > tdoc.beginAt,
    showSelfRecord: () => true,
    showRecord: (tdoc, now) => now > tdoc.endAt && !isLocked(tdoc),
    stat(tdoc, journal: CodelinkJournal[]) {
        const naccept = Counter<number>();
        const npending = Counter<number>();
        const display: Record<number, CodelinkDetail> = {};
        const detail: Record<number, CodelinkDetail> = {};
        let scoreSum = 0;
        let acceptCount = 0;
        let firstBloodCount = 0;
        let time = 0;
        const lockAt = isLocked(tdoc) ? tdoc.lockAt : null;
        for (const j of journal) {
            if (!tdoc.pids.includes(j.pid)) continue;
            const pIndex = tdoc.pids.indexOf(j.pid);
            if (pIndex >= problemData.length) continue;
            const pData = problemData[pIndex];

            if (!this.submitAfterAccept && display[j.pid]?.status === STATUS.STATUS_ACCEPTED) continue;
            if (![STATUS.STATUS_ACCEPTED, STATUS.STATUS_COMPILE_ERROR, STATUS.STATUS_FORMAT_ERROR, STATUS.STATUS_CANCELED].includes(j.status)) {
                naccept[j.pid]++;
            }
            const real = Math.floor((j.rid.getTimestamp().getTime() - tdoc.beginAt.getTime()) / 1000);
            const realMinute = Math.floor((j.rid.getTimestamp().getTime() - tdoc.beginAt.getTime()) / (1000*60));
            let scorewf = Math.max(pData.min, pData.base - realMinute * pData.perMin - naccept[j.pid] * 50);
            if( j.status !== STATUS.STATUS_ACCEPTED) {
                scorewf = 0;
            }
            detail[j.pid] = {
                ...j, naccept: naccept[j.pid], time: real, score: scorewf, scorewf
            };
            if (lockAt && j.rid.getTimestamp() > lockAt) {
                npending[j.pid]++;
                // FIXME this is tricky
                // @ts-ignore
                display[j.pid] ||= {};
                display[j.pid].npending = npending[j.pid];
                continue;
            }
            display[j.pid] = detail[j.pid];
        }
        for (const d of Object.values(display).filter((i) => i.status === STATUS.STATUS_ACCEPTED)) {
            acceptCount++;
            time += d.time;
            scoreSum += d.score;
        }
        return {
            acceptCount, firstBloodCount, time, scoreSum, detail, display,
        };
    },
    async scoreboardHeader(config, _, tdoc, pdict) {
        const columns: ScoreboardRow = [
            { type: 'rank', value: '#' },
            { type: 'user', value: _('User') },
        ];
        if (config.isExport && config.showDisplayName) {
            columns.push({ type: 'email', value: _('Email') });
            columns.push({ type: 'string', value: _('School') });
            columns.push({ type: 'string', value: _('Name') });
            columns.push({ type: 'string', value: _('Student ID') });
        }
        columns.push({ type: 'total_score', value: _('Total Score') });
        columns.push({ type: 'solved', value: `${_('Accept')}\n${_('Total Time')}` });
        for (let i = 1; i <= tdoc.pids.length; i++) {
            const pid = tdoc.pids[i - 1];
            pdict[pid].nAccept = pdict[pid].nSubmit = 0;
            if (config.isExport) {
                columns.push(
                    {
                        type: 'string',
                        value: '#{0} {1}'.format(i, pdict[pid].title),
                    },
                    {
                        type: 'time',
                        value: '#{0} {1}'.format(i, _('Penalty (Minutes)')),
                    },
                );
            } else {
                columns.push({
                    type: 'problem',
                    value: getAlphabeticId(i - 1),
                    raw: pid,
                });
            }
        }
        return columns;
    },
    async scoreboardRow(config, _, tdoc, pdict, udoc, rank, tsdoc, meta) {
        const row: ScoreboardRow = [
            { type: 'rank', value: rank.toString() },
            { type: 'user', value: udoc.uname, raw: tsdoc.uid },
        ];
        if (config.isExport && config.showDisplayName) {
            row.push({ type: 'email', value: udoc.mail });
            row.push({ type: 'string', value: udoc.school || '' });
            row.push({ type: 'string', value: udoc.displayName || '' });
            row.push({ type: 'string', value: udoc.studentId || '' });
        }
        row.push({ type: 'total_score', value: tsdoc.scoreSum || 0 });
        row.push({
            type: 'time',
            value: `${tsdoc.acceptCount || 0}\n${formatSeconds(tsdoc.time || 0.0, false)}`,
            hover: formatSeconds(tsdoc.time || 0.0),
        });
        const accepted = {};
        for (const s of tsdoc.journal || []) {
            if (!pdict[s.pid]) continue;
            if (config.lockAt && s.rid.getTimestamp() > config.lockAt) continue;
            pdict[s.pid].nSubmit++;
            if (s.status === STATUS.STATUS_ACCEPTED && !accepted[s.pid]) {
                pdict[s.pid].nAccept++;
                accepted[s.pid] = true;
            }
        }
        const tsddict = (config.lockAt ? tsdoc.display : tsdoc.detail) || {};
        for (const pid of tdoc.pids) {
            const doc = tsddict[pid] || {} as Partial<CodelinkDetail>;
            const accept = doc.status === STATUS.STATUS_ACCEPTED;
            const colTime = accept ? formatSeconds(doc.real, false).toString() : '';
            const colPenalty = doc.rid ? Math.ceil(doc.penalty / 60).toString() : '';
            if (config.isExport) {
                row.push(
                    { type: 'string', value: colTime },
                    { type: 'string', value: colPenalty },
                );
            } else {
                let value = '';
                if (doc.rid) value = `-${doc.naccept}`;
                if (accept) value = `${doc.naccept ? `+${doc.naccept}` : '<span class="icon icon-check"></span>'}\n${doc.score}`;
                else if (doc.npending) value += `${value ? ' ' : ''}<span style="color:orange">+${doc.npending}</span>`;
                row.push({
                    type: 'record',
                    score: accept ? 100 : 0,
                    value,
                    hover: accept ? formatSeconds(doc.time) : '',
                    raw: doc.rid,
                    style: accept && doc.rid.getTimestamp().getTime() === meta?.first?.[pid]
                        ? 'background-color: rgb(217, 240, 199);'
                        : undefined,
                });
            }
        }
        return row;
    },
    async scoreboard(config, _, tdoc, pdict, cursor) {
        const rankedTsdocs = await this.ranked(tdoc, cursor);
        const uids = rankedTsdocs.map(([, tsdoc]) => tsdoc.uid);
        const udict = await UserModel.getListForRender(tdoc.domainId, uids, config.showDisplayName ? ['displayName'] : []);
        // Find first accept
        const first = {};
        const data = await DocumentModel.collStatus.aggregate([
            {
                $match: {
                    domainId: tdoc.domainId,
                    docType: DocumentModel.TYPE_CONTEST,
                    docId: tdoc.docId,
                    acceptCount: { $gte: 1 },
                },
            },
            { $project: { r: { $objectToArray: '$detail' } } },
            { $unwind: '$r' },
            { $match: { 'r.v.status': STATUS.STATUS_ACCEPTED } },
            { $group: { _id: '$r.v.pid', first: { $min: '$r.v.rid' } } },
        ]).toArray() as any[];
        for (const t of data) first[t._id] = t.first.getTimestamp().getTime();

        const columns = await this.scoreboardHeader(config, _, tdoc, pdict);
        const rows: ScoreboardRow[] = [
            columns,
            ...await Promise.all(rankedTsdocs.map(
                ([rank, tsdoc]) => this.scoreboardRow(
                    config, _, tdoc, pdict, udict[tsdoc.uid], rank, tsdoc, { first },
                ),
            )),
        ];
        return [rows, udict];
    },
    async ranked(tdoc, cursor) {
        // Find first accept
        const first = {};
        const data = await DocumentModel.collStatus.aggregate([
            {
                $match: {
                    domainId: tdoc.domainId,
                    docType: DocumentModel.TYPE_CONTEST,
                    docId: tdoc.docId,
                    acceptCount: { $gte: 1 },
                },
            },
            { $project: { r: { $objectToArray: '$detail' } } },
            { $unwind: '$r' },
            { $match: { 'r.v.status': STATUS.STATUS_ACCEPTED } },
            { $group: { _id: '$r.v.pid', first: { $min: '$r.v.rid' } } },
        ]).toArray() as any[];
        for (const t of data) first[t._id] = t.first.getTimestamp().getTime();
        
        const tsdocs = await cursor.toArray();

        for (const tsdoc of tsdocs) { 
            tsdoc.firstBloodCount = 0;
            tsdoc.scoreSum = 0;
            if (!tsdoc.detail) continue;
            for (const pid of tdoc.pids) {
                const problemDetail = tsdoc.detail[pid] as CodelinkDetail;
                if (!problemDetail) continue;

                if (problemDetail.rid.getTimestamp().getTime() === first[pid]) {
                    const pidx = tdoc.pids.indexOf(pid);
                    const bonus = problemData[pidx].base * FIRST_BLOOD_BONUS_RATE;
                    console.log(`First blood bonus for ${pid}: ${bonus}`);
                    tsdoc.firstBloodCount++;
                    problemDetail.score = (problemDetail.scorewf || 0) + bonus;
                }
                tsdoc.scoreSum = (tsdoc.scoreSum || 0) + problemDetail.score;
            }
        }

        tsdocs.sort((a, b) => {
            if ((b.scoreSum || 0) !== (a.scoreSum || 0)) return (b.scoreSum || 0) - (a.scoreSum || 0);
            if ((b.acceptCount || 0) !== (a.acceptCount || 0)) return (b.acceptCount || 0) - (a.acceptCount || 0);
            if ((b.firstBloodCount || 0) !== (a.firstBloodCount || 0)) return (b.firstBloodCount || 0) - (a.firstBloodCount || 0);
            return (a.time || 0) - (b.time || 0);
        });

        const rankedTsdocs = [];
        let lastRank = 1;
        for (let i = 0; i < tsdocs.length; i++) {
            // if (i > 0 && (
            //     (tsdocs[i].score || 0) !== (tsdocs[i - 1].score || 0)
            //     || (tsdocs[i].acceptCount || 0) !== (tsdocs[i - 1].acceptCount || 0)
            //     || (tsdocs[i].firstBloodCount || 0) !== (tsdocs[i - 1].firstBloodCount || 0)
            //     || (tsdocs[i].time || 0) !== (tsdocs[i - 1].time || 0)
            // )) {
            //     lastRank = i + 1;
            // }
            lastRank = i + 1;
            rankedTsdocs.push([lastRank, tsdocs[i]]);
        }
        return rankedTsdocs;
        // return await db.ranked(cursor, (a, b) => a.accept === b.accept && a.time === b.time);
    },
    applyProjection(tdoc, rdoc) {
        if (isDone(tdoc)) return rdoc;
        delete rdoc.time;
        delete rdoc.memory;
        rdoc.testCases = [];
        rdoc.judgeTexts = [];
        delete rdoc.subtasks;
        delete rdoc.score;
        return rdoc;
    },
});


export async function apply(ctx: Context) {
    ContestModel.RULES.ledo = codelink;
}

