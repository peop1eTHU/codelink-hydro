import {
    Schema, STATUS, DomainModel as domain, ContestModel as contest,
    Tdoc, Udoc, db, UserModel, PRIV, ProblemModel as problem,
    Counter, UserModel as user, Handler,
    Context, ObjectID, query, Types, SettingModel
} from 'hydrooj';
import { langs } from 'hydrooj/src/model/setting';
import { NumericDictionary, unionWith } from 'lodash';

type ND = NumericDictionary<number>;

const POINTS_MAPPING = {
    1: 3,
    2: 2,
    3: 1,
    4: 0,
};

interface PointDef {
    run(domainIds: string[], udict: ND, report: Function): Promise<void>;
    hidden: boolean;
    base: number;
}

const { log, max, min } = Math;

function rating(users: { old: number, uid: number, rank: number }[]): { new: number, uid: number }[] {
    let result = [];
    for (const user of users) {
        const newRating = user.old + POINTS_MAPPING[min(user.rank, 4)];
        result.push({ new: newRating, uid: user.uid });
    }
    return result;
}

export const PointTypes: Record<string, PointDef> = {
    contest: {
        async run(domainIds, udict, report) {
            const contests: Tdoc[] = await contest.getMulti('', { domainId: { $in: domainIds }, rated: true })
                .toArray() as any;
            if (contests.length) await report({ message: `Found ${contests.length} contests in ${domainIds[0]}` });
            for (const tdoc of contests.reverse()) {
                const start = Date.now();
                const query = {
                    docId: tdoc.docId,
                    journal: { $ne: null },
                };
                if (!await contest.countStatus(tdoc.domainId, query)) continue;
                const cursor = contest.getMultiStatus(tdoc.domainId, query).sort(contest.RULES[tdoc.rule].statusSort);
                const rankedTsdocs = await contest.RULES[tdoc.rule].ranked(tdoc, cursor);
                const users = rankedTsdocs.map((i) => ({ uid: i[1].uid, rank: i[0], old: udict[i[1].uid] }));
                for (const udoc of rating(users)) udict[udoc.uid] = udoc.new;
                await report({
                    case: {
                        status: STATUS.STATUS_ACCEPTED,
                        message: `Contest ${tdoc.title} finished`,
                        time: Date.now() - start,
                        memory: 0,
                        score: 0,
                    },
                });
            }
        },
        hidden: false,
        base: 0,
    },
};

async function runCalcPointInDomain(domainId: string, report: Function) {
    const results: Record<keyof typeof PointTypes, ND> = {};
    const udict = Counter();
    await db.collection('domain.user').updateMany({ domainId }, { $set: { pointInfo: {} } });
    for (const type in PointTypes) {
        results[type] = new Proxy({}, { get: (self, key) => self[key] || PointTypes[type].base });
        await PointTypes[type].run([domainId], results[type], report);
        const bulk = db.collection('domain.user').initializeUnorderedBulkOp();
        for (const uid in results[type]) {
            const udoc = await UserModel.getById(domainId, +uid);
            if (!udoc?.hasPriv(PRIV.PRIV_USER_PROFILE)) continue;
            bulk.find({ domainId, uid: +uid }).updateOne({ $set: { [`pointInfo.${type}`]: results[type][uid] } });
            udict[+uid] += results[type][uid];
        }
        if (bulk.batches.length) await bulk.execute();
    }
    await domain.setMultiUserInDomain(domainId, {}, { point: 0 });
    const bulk = db.collection('domain.user').initializeUnorderedBulkOp();
    for (const uid in udict) {
        bulk.find({ domainId, uid: +uid }).upsert().update({ $set: { point: Math.max(0, udict[uid]) } });
    }
    if (bulk.batches.length) await bulk.execute();
    await runCalcCodelinkRankInDomain(domainId, report);
}

export async function runCalcCodelinkRankInDomain(domainId: string, report: Function) {
    await domain.setMultiUserInDomain(domainId, {}, { codelink_rank: null });
    let last = { point: null };
    let codelink_rank = 0;
    let count = 0;
    const coll = db.collection('domain.user');
    const filter = { uid: { $nin: [0, 1], $gt: -1000 } };
    const ducur = domain.getMultiUserInDomain(domainId, filter)
        .project<{ _id: ObjectID, point: number }>({ point: 1 })
        .sort({ point: -1 });
    let bulk = coll.initializeUnorderedBulkOp();
    for await (const dudoc of ducur) {
        count++;
        dudoc.point ||= null;
        if (dudoc.point !== last.point) codelink_rank = count;
        bulk.find({ _id: dudoc._id }).updateOne({ $set: { codelink_rank } });
        last = dudoc;
        if (count % 100 === 0) report({ message: `#${count}: codelink_rank ${codelink_rank}` });
    }
    if (!count) return;
    await bulk.execute();
}

export async function runCalcPoint({ domainId }, report: Function) {
    if (!domainId) {
        const domains = await domain.getMulti().toArray();
        await report({ message: `Found ${domains.length} domains` });
        for (const i in domains) {
            const start = new Date().getTime();
            await runCalcPointInDomain(domains[i]._id, report);
            await report({
                case: {
                    status: STATUS.STATUS_ACCEPTED,
                    message: `Domain ${domains[i]._id} finished`,
                    time: new Date().getTime() - start,
                    memory: 0,
                    score: 0,
                },
                progress: Math.floor(((+i + 1) / domains.length) * 100),
            });
        }
    } else await runCalcPointInDomain(domainId, report);
    return true;
}

async function createMatches(domainId: string, groupName: string, templateTid: string, report: Function) {
    await report({ message: `Fetching template contest with ID: ${templateTid}` });
    const templateTdoc = await contest.get(domainId, new ObjectID(templateTid));
    if (!templateTdoc) {
        throw new Error(`Template contest with ID '${templateTid}' not found.`);
    }
    if (templateTdoc.pids.length !== 7) {
        await report({ message: `Warning: Template contest does not have exactly 7 problems (found ${templateTdoc.pids.length}).` });
    }
    await report({ message: `Using "${templateTdoc.title}" as template.` });

    await report({ message: `Fetching user IDs from group '${groupName}'...` });
    const groupDoc = await user.collGroup.findOne({ domainId, name: groupName });
    if (!groupDoc || !groupDoc.uids?.length) {
        await report({ message: `Group '${groupName}' not found or has no members.` });
        return;
    }
    const groupUserIds = groupDoc.uids;

    await report({ message: `Fetching and sorting points for ${groupUserIds.length} users in the group...` });
    const userPoints = await domain.getMultiUserInDomain(domainId, {
        uid: { $in: groupUserIds },
        point: { $exists: true },
    }).project({ uid: 1, point: 1 }).toArray();

    userPoints.sort((a, b) => b.point - a.point);
    await report({ message: `Found ${userPoints.length} users with points. Starting to create matches.` });


    let matchCounter = 1;
    for (let i = 0; i < userPoints.length; i += 4) {
        const groupOfFour = userPoints.slice(i, i + 4);

        if (groupOfFour.length < 4) {
            await report({ message: `Warning: Not enough users for a complete match (only ${groupOfFour.length} users).` });
        }

        const uidsForMatch = groupOfFour.map(u => u.uid);
        const udocs = await user.getList(domainId, uidsForMatch);

        
        const contestTitle = `${templateTdoc.title} - Group ${matchCounter}`;

        await report({ message: `Creating match #${matchCounter} for users: ${uidsForMatch.map(uid => udocs[uid].uname).join(', ')}` });
        const assignUserIdsAsString = uidsForMatch.map(uid => uid.toString());

        const newContest: Partial<Tdoc> = {
            title: contestTitle,
            content: templateTdoc.content,
            pids: templateTdoc.pids,
            rule: templateTdoc.rule,
            beginAt: templateTdoc.beginAt,
            endAt: templateTdoc.endAt,
            owner: templateTdoc.owner,
            assign: assignUserIdsAsString,
            rated: true,
            maintainer: templateTdoc.maintainer,
            langs: templateTdoc.langs,
            allowViewCode: templateTdoc.allowViewCode,
            autoHide: templateTdoc.autoHide,
            duration: templateTdoc.duration,
            lockAt: templateTdoc.lockAt,
            _code: templateTdoc._code,
        };

        try {
            const newTid = await contest.add(
                domainId,
                newContest.title,
                newContest.content,
                newContest.owner,
                newContest.rule,
                newContest.beginAt,
                newContest.endAt,
                newContest.pids,
                newContest.rated,
                {
                    assign: newContest.assign, 
                    maintainer: newContest.maintainer,
                    langs: newContest.langs,
                    allowViewCode: newContest.allowViewCode,
                    autoHide: newContest.autoHide,
                    duration: newContest.duration,
                    lockAt: newContest.lockAt,
                    _code: newContest._code,
                },
            );
            await report({ message: `Successfully created contest #${matchCounter} with ID: ${newTid}` });
        } catch (e) {
            await report({ message: `Failed to create contest #${matchCounter}. Error: ${e.message}` });
        }
        matchCounter++;
    }
}

export async function runCreateMatches({ domainId, groupName, templateTid }, report: Function) {
    if (!domainId || !groupName || !templateTid) {
        throw new Error('domainId, groupName, and templateTid are required.');
    }
    await createMatches(domainId, groupName, templateTid, report);
    return true;
}

class DomainCodelinkRankHandler extends Handler {
    @query('page', Types.PositiveInt, true)
    async get(domainId: string, page = 1) {
        const [dudocs, upcount, ucount] = await this.paginate(
            domain.getMultiUserInDomain(domainId, { uid: { $gt: 1 } }).sort({ point: -1 }),
            page,
            'codelink_ranking',
        );
        const udict = await user.getList(domainId, dudocs.map((dudoc) => dudoc.uid));
        const udocs = dudocs.map((i) => udict[i.uid]);
        this.response.template = 'codelink_ranking.html';
        this.response.body = {
            udocs, upcount, ucount, page,
        };
    }
}

export async function apply(ctx: Context) {
    SettingModel.DomainUserSetting(
        SettingModel.Setting('setting_storage', 'point', 0, 'number', 'Point', null, SettingModel.FLAG_HIDDEN | SettingModel.FLAG_DISABLED),
        SettingModel.Setting('setting_storage', 'codelink_rank', 0, 'number', 'Codelink Rank', null, SettingModel.FLAG_DISABLED | SettingModel.FLAG_HIDDEN),
    );
    SettingModel.SystemSetting(
        SettingModel.Setting('setting_basic', 'pagination.codelink_ranking', 100, 'number', 'pagination.codelink_ranking', 'Users per page'),
    );
    ctx.addScript(
        'calcPoint', 'Calculate point of a domain, or all domains',
        Schema.object({ domainId: Schema.string() }), runCalcPoint,
    );
    ctx.addScript(
        'createMatches', 'Create contests for a group based on a template contest',
        Schema.object({
            domainId: Schema.string(),
            groupName: Schema.string(),
            templateTid: Schema.string(),
        }), runCreateMatches,
    );

    ctx.Route('codelink_ranking', '/codelink_ranking', DomainCodelinkRankHandler);
    ctx.injectUI('Nav', 'codelink_ranking', { prefix: 'codelink' });
    ctx.i18n.load('zh', {
        codelink_ranking: '代码链接',
    });
    ctx.i18n.load('en', {
        codelink_ranking: 'Codelink',
    });
}

